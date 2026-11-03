package dlh.bridge;

import com.dukascopy.api.IAccount;
import com.dukascopy.api.IBar;
import com.dukascopy.api.IContext;
import com.dukascopy.api.IEngine;
import com.dukascopy.api.IMessage;
import com.dukascopy.api.IOrder;
import com.dukascopy.api.IStrategy;
import com.dukascopy.api.ITick;
import com.dukascopy.api.Instrument;
import com.dukascopy.api.Period;
import com.dukascopy.api.system.ClientFactory;
import com.dukascopy.api.system.IClient;
import com.dukascopy.api.system.ISystemListener;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.json.JSONObject;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Trades a Dukascopy account through the JForex SDK and serves it over HTTP on localhost,
 * for trading/brokers/dukascopy.py:
 *
 * <pre>
 *   GET  /health                          connected, account id
 *   GET  /account                         balance, equity, currency
 *   GET  /tick?symbol=XAUUSD              last bid / ask
 *   POST /orders                          {label, symbol, side, qty, stop, target, min_size?}: market order
 *                                         with stop and target attached; an existing label is
 *                                         returned as is (duplicate: true), never sent twice
 *   GET  /orders/{label}?symbol=&id=&since=  the order: seen by this process, else from the account
 *                                         history by its Dukascopy id, else by label since `since` (epoch ms)
 *   POST /orders/{label}/close?symbol=    close it at market (manual checks only)
 * </pre>
 *
 * Quantities are in units (ounces for XAUUSD, base currency for FX); JForex wants millions.
 * Engine calls run on the strategy thread (IContext.executeTask), as the SDK requires; history
 * lookups block on the server, so they run on the HTTP thread instead (on the strategy thread
 * they never return). Commission is reported as a positive cost.
 * Settings come from the environment (run.sh loads .env): DUKASCOPY_DEMO_USER,
 * DUKASCOPY_DEMO_PASSWORD, DUKASCOPY_JNLP_URL, TRADING_BRIDGE_PORT, DUKASCOPY_SYMBOLS.
 */
public final class Bridge implements IStrategy {

    private static final String DEMO_JNLP = "https://platform.dukascopy.com/demo/jforex.jnlp";
    private static final Pattern LABEL = Pattern.compile("[A-Za-z][A-Za-z0-9_]{0,255}");
    private static final long CALL_TIMEOUT_S = 30;
    private static final long FILL_WAIT_MS = 5_000;

    private final CountDownLatch started = new CountDownLatch(1);
    private final Map<String, String> rejections = new ConcurrentHashMap<>();
    // Every order this process has seen, by label: a closed order is no longer in the engine
    private final Map<String, IOrder> known = new ConcurrentHashMap<>();
    private volatile IContext context;
    private volatile IClient client;

    public static void main(String[] args) throws Exception {
        String user = require("DUKASCOPY_DEMO_USER");
        String password = require("DUKASCOPY_DEMO_PASSWORD");
        String jnlp = env("DUKASCOPY_JNLP_URL", DEMO_JNLP);
        int port = Integer.parseInt(env("TRADING_BRIDGE_PORT", "8765"));
        Set<Instrument> instruments = new HashSet<>();
        for (String symbol : env("DUKASCOPY_SYMBOLS", "XAUUSD").split(",")) {
            if (!symbol.isBlank()) instruments.add(instrument(symbol.trim()));
        }
        new Bridge().run(jnlp, user, password, instruments, port);
    }

    private void run(String jnlp, String user, String password, Set<Instrument> instruments, int port)
            throws Exception {
        client = ClientFactory.getDefaultInstance();
        client.setSystemListener(new ISystemListener() {
            @Override public void onStart(long processId) { log("strategy started"); }
            @Override public void onStop(long processId) { log("strategy stopped"); }
            @Override public void onConnect() { log("connected"); }
            @Override public void onDisconnect() {
                log("disconnected, reconnecting");
                reconnect(jnlp, user, password);
            }
        });
        log("logging in to " + jnlp + " as " + user);
        connect(jnlp, user, password);
        client.setSubscribedInstruments(instruments);
        client.startStrategy(this);
        if (!started.await(60, TimeUnit.SECONDS)) throw new IllegalStateException("strategy did not start");

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.createContext("/", this::handle);
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();
        log("serving on http://127.0.0.1:" + port + " for " + instruments);
    }

    private void connect(String jnlp, String user, String password) throws Exception {
        client.connect(jnlp, user, password);
        for (int i = 0; i < 20 && !client.isConnected(); i++) Thread.sleep(1_000);
        if (!client.isConnected()) {
            throw new IllegalStateException("login failed: check the demo login, and that this machine's IP "
                    + "is registered on the account (otherwise Dukascopy asks for a PIN)");
        }
    }

    private void reconnect(String jnlp, String user, String password) {
        new Thread(() -> {
            for (int attempt = 1; !client.isConnected(); attempt++) {
                try {
                    if (client.isReconnectAllowed()) client.reconnect();
                    else connect(jnlp, user, password);
                    Thread.sleep(10_000);
                } catch (Exception e) {
                    log("reconnect attempt " + attempt + " failed: " + e.getMessage());
                    sleep(Math.min(300_000, 10_000L * attempt));
                }
            }
        }, "reconnect").start();
    }

    // --- IStrategy: only used to get the context and record rejections ---

    @Override
    public void onStart(IContext context) throws com.dukascopy.api.JFException {
        this.context = context;
        for (IOrder o : context.getEngine().getOrders()) known.put(o.getLabel(), o);
        started.countDown();
    }
    @Override public void onTick(Instrument instrument, ITick tick) { }
    @Override public void onBar(Instrument instrument, Period period, IBar askBar, IBar bidBar) { }
    @Override public void onAccount(IAccount account) { }
    @Override public void onStop() { }

    @Override
    public void onMessage(IMessage message) {
        IOrder order = message.getOrder();
        if (order == null) return;
        known.put(order.getLabel(), order);
        switch (message.getType()) {
            case ORDER_SUBMIT_REJECTED, ORDER_FILL_REJECTED, ORDER_CHANGED_REJECTED, ORDER_CLOSE_REJECTED -> {
                rejections.put(order.getLabel(), message.getType() + ": " + message.getContent());
                log(order.getLabel() + " " + message.getType() + ": " + message.getContent());
            }
            case ORDER_FILL_OK, ORDER_CLOSE_OK -> log(order.getLabel() + " " + message.getType()
                    + " at " + (message.getType() == IMessage.Type.ORDER_FILL_OK ? order.getOpenPrice()
                    : order.getClosePrice()));
            default -> { }
        }
    }

    // --- HTTP ---

    private void handle(HttpExchange ex) throws IOException {
        int status = 200;
        JSONObject body;
        try {
            String method = ex.getRequestMethod();
            String[] path = ex.getRequestURI().getPath().replaceAll("^/+|/+$", "").split("/");
            Map<String, String> query = query(ex.getRequestURI().getRawQuery());
            if (path[0].equals("health")) {
                body = new JSONObject().put("connected", client.isConnected())
                        .put("account_id", call(() -> context.getAccount().getAccountId()));
            } else if (path[0].equals("account")) {
                body = call(() -> {
                    IAccount a = context.getAccount();
                    return new JSONObject().put("balance", a.getBalance()).put("equity", a.getEquity())
                            .put("currency", a.getAccountCurrency().getCurrencyCode())
                            .put("account_id", a.getAccountId());
                });
            } else if (path[0].equals("tick")) {
                Instrument instrument = instrument(param(query, "symbol"));
                body = call(() -> {
                    ITick tick = context.getHistory().getLastTick(instrument);
                    if (tick == null) throw new IllegalStateException("no tick yet for " + instrument);
                    return new JSONObject().put("bid", tick.getBid()).put("ask", tick.getAsk()).put("time", tick.getTime());
                });
            } else if (path[0].equals("orders") && path.length == 1 && method.equals("POST")) {
                body = submit(new JSONObject(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
            } else if (path[0].equals("orders") && path.length == 2 && method.equals("GET")) {
                IOrder order = find(path[1], instrument(param(query, "symbol")), query.get("id"),
                        Long.parseLong(query.getOrDefault("since", "0")));
                if (order == null) {
                    status = 404;
                    body = new JSONObject().put("error", "no order " + path[1])
                            .put("reason", rejections.get(path[1]));
                } else {
                    body = json(order);
                }
            } else if (path[0].equals("orders") && path.length == 3 && path[2].equals("close") && method.equals("POST")) {
                String label = path[1];
                body = call(() -> {
                    IOrder order = context.getEngine().getOrder(label);
                    if (order == null) throw new IllegalArgumentException("no active order " + label);
                    if (order.getState() == IOrder.State.FILLED) order.close();
                    return new JSONObject().put("label", label).put("closing", true);
                });
            } else {
                status = 404;
                body = new JSONObject().put("error", "unknown endpoint " + method + " " + ex.getRequestURI().getPath());
            }
        } catch (IllegalArgumentException e) {
            status = 400;
            body = new JSONObject().put("error", e.getMessage());
        } catch (Exception e) {
            status = 500;
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            body = new JSONObject().put("error", cause.getClass().getSimpleName() + ": " + cause.getMessage());
            log(ex.getRequestMethod() + " " + ex.getRequestURI() + " failed: " + body.getString("error"));
        }
        byte[] bytes = body.toString().getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = ex.getResponseBody()) { out.write(bytes); }
    }

    private JSONObject submit(JSONObject req) throws Exception {
        String label = req.getString("label");
        if (!LABEL.matcher(label).matches()) {
            throw new IllegalArgumentException("label must be a letter then letters, digits or _: " + label);
        }
        Instrument instrument = instrument(req.getString("symbol"));
        String side = req.getString("side");
        if (!side.equals("long") && !side.equals("short")) throw new IllegalArgumentException("side must be long or short");
        double stop = req.getDouble("stop");
        double target = req.optDouble("target", 0.0);
        if (Double.isNaN(target)) target = 0.0;
        // Round down to the instrument's size step; below its minimum the order is not sent
        double step = instrument.getTradeAmountIncrement();
        double units = req.optBoolean("min_size") ? instrument.getMinTradeAmount()
                : Math.floor(req.getDouble("qty") / step + 1e-9) * step;
        if (units < instrument.getMinTradeAmount()) {
            return new JSONObject().put("label", label).put("state", "REJECTED")
                    .put("reason", "size " + req.getDouble("qty") + " below the minimum " + instrument.getMinTradeAmount());
        }
        long since = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7);
        IOrder existing = find(label, instrument, null, since);
        if (existing != null) return json(existing).put("duplicate", true);

        double sl = stop, tp = target;
        IOrder order = call(() -> context.getEngine().submitOrder(label, instrument,
                side.equals("long") ? IEngine.OrderCommand.BUY : IEngine.OrderCommand.SELL,
                units / 1e6, 0, 5, sl, tp));
        known.put(label, order);
        // Wait briefly for the fill so the caller sees fill or rejection right away
        long deadline = System.currentTimeMillis() + FILL_WAIT_MS;
        while (System.currentTimeMillis() < deadline) {
            IOrder.State state = call(order::getState);
            if (state != IOrder.State.CREATED && state != IOrder.State.OPENED) break;
            Thread.sleep(200);
        }
        return call(() -> json(order));
    }

    /** An order by label: one this process has seen, an active one, then the account history (by its
     *  Dukascopy id when known, else by label since `since`). */
    private IOrder find(String label, Instrument instrument, String id, long since) throws Exception {
        IOrder order = known.get(label);
        if (order == null) order = call(() -> context.getEngine().getOrder(label));
        if (order == null && id != null && !id.isBlank()) {
            order = context.getHistory().getHistoricalOrderById(id);
            if (order != null && !label.equals(order.getLabel())) order = null;
        }
        if (order == null) {
            long from = Math.max(since, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30));
            for (IOrder o : context.getHistory().getOrdersHistory(instrument, from, System.currentTimeMillis())) {
                if (label.equals(o.getLabel())) order = o;
            }
        }
        if (order != null) known.put(label, order);
        return order;
    }

    private JSONObject json(IOrder o) {
        JSONObject j = new JSONObject()
                .put("label", o.getLabel()).put("id", o.getId()).put("state", o.getState().name())
                .put("side", o.isLong() ? "long" : "short")
                .put("qty", o.getAmount() * 1e6).put("requested_qty", o.getRequestedAmount() * 1e6)
                .put("open_price", o.getOpenPrice()).put("fill_time", o.getFillTime())
                .put("close_price", o.getClosePrice()).put("close_time", o.getCloseTime())
                .put("stop", o.getStopLossPrice()).put("target", o.getTakeProfitPrice())
                .put("pnl", o.getProfitLossInAccountCurrency()).put("commission", Math.abs(o.getCommission()));
        String reason = rejections.get(o.getLabel());
        if (reason != null) j.put("reason", reason);
        return j;
    }

    private <T> T call(Callable<T> task) throws Exception {
        if (context == null || !client.isConnected()) throw new IllegalStateException("not connected to Dukascopy");
        return context.executeTask(task).get(CALL_TIMEOUT_S, TimeUnit.SECONDS);
    }

    // --- helpers ---

    /** XAUUSD -> XAU/USD; anything Instrument.fromString knows as is. */
    static Instrument instrument(String symbol) {
        Instrument i = Instrument.fromString(symbol);
        if (i == null && symbol.length() == 6) i = Instrument.fromString(symbol.substring(0, 3) + "/" + symbol.substring(3));
        if (i == null) throw new IllegalArgumentException("unknown symbol " + symbol);
        return i;
    }

    private static Map<String, String> query(String raw) {
        Map<String, String> q = new HashMap<>();
        if (raw == null) return q;
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) q.put(URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                    URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
        }
        return q;
    }

    private static String param(Map<String, String> query, String name) {
        String value = query.get(name);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("missing ?" + name + "=");
        return value;
    }

    private static String require(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            System.err.println(name + " is not set (put it in .env)");
            System.exit(2);
        }
        return value;
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private static void log(String message) {
        System.out.println(java.time.Instant.now() + " " + message);
    }
}
