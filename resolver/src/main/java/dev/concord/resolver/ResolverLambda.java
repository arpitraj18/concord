package dev.concord.resolver;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import java.util.Map;

/** Invoked by EventBridge Scheduler (e.g. every minute): one sweep per invocation. */
public final class ResolverLambda implements RequestHandler<Map<String, Object>, Map<String, Integer>> {

    private static final class Holder {
        static final Resolver RESOLVER = ResolverMain.fromEnv(System.getenv());
    }

    @Override
    public Map<String, Integer> handleRequest(Map<String, Object> event, Context context) {
        Map<String, Integer> out = new java.util.HashMap<>();
        Holder.RESOLVER.sweep().counts().forEach((k, v) -> out.put(k.name(), v));
        return out;
    }
}
