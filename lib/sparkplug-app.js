/* ACS Sparkplug App library.
 * Sparkplug App class.
 * Copyright 2023 AMRC.
 */

//import process      from "process";

import Immutable    from "immutable";
import rx           from "rxjs";

import { SpB, Topic }       from "@amrc-factoryplus/utilities";

import { SparkplugDevice }  from "./sparkplug-device.js";

export class SparkplugApp {
    constructor (opts) {
        this.fplus = opts.fplus;
        this.debug = opts.fplus.debug;
    }

    async init () {
        const fplus = this.fplus;

        const mqtt = this.mqtt = await fplus.MQTT.mqtt_client({});

        this.packets = this._init_packets();
        this.topics = new Map();
        this._init_subscriptions();

        return this;
    }

    /* Sparkplug packets received from the broker */
    _init_packets () {
        return rx.fromEvent(this.mqtt, "message", 
            (t, p) => ({
                topic:      t, 
                payload:    SpB.decodePayload(p),
            }))
        .pipe(rx.share());
    }

    _init_subscriptions () {
        const log = this.debug.log.bind(this.debug, "mqttsub");

        /* Subscription state changes */
        this.sub_changes = new rx.Subject();

        /* Current state of our subscriptions */
        const sub_state = this._init_sub_state(log);

        /* Start handling subscription requests */
        this._process_subscriptions(log, sub_state);
    }

    /* Creates a sequence representing the current state of our
     * subscriptions. Subscription states are:
     *      opening     Subscription has been requested
     *      open        We are subscribed
     *      failed      Subscription has failed
     *      retry       Subscription should be retried
     *      closing     Unsubscription has been requested
     *      (closed     Unsubscription complete)
     * Unsubscribed topics are removed from the map but the "closed"
     * state is used internally.
     */
    _init_sub_state (log) {
        const mqtt  = this.mqtt;

        /* Handle the state transitions. The values of this sequence are
         * [state, change] where state is a Map holding the current
         * state of our subscriptions and change is the last change.
         * Incoming changes that don't change state are suppressed. */
        return this.sub_changes.pipe(
            rx.tap(v => log("Subscription state change %o", v)),
            rx.scan(([states], change) => {
                const [topic, state] = change;
                const old = states.get(topic, "closed");
                log("State update %s -> %s", old, state);
                if (old == state) 
                    return [states, null];

                switch (`${old}|${state}`) {
                    case "open|opening":
                    case "failed|opening":
                    case "closed|closing":
                    case "opening|closed":
                    case "closing|open":
                        return [states, null];
                    case "open|closed":
                    case "failed|closed":
                    case "closed|open":
                        log("Unexpected state change %s->%s for %s",
                            old, state, topic);
                }
                if (state == "closed")
                    return [states.delete(topic), change];
                return [states.set(topic, state), change];
            }, [Immutable.Map(), null]),
            rx.filter(v => v[1] != null),
            rx.tap(([st, ch]) => log("Sub update %o => %o", ch, st.toJS())),
            rx.share({ resetOnRefCountZero: false }),
        );
    }

    /* Handle the subscription state changes and take appropriate
     * action. Feed more state changes into sub_changes as needed. */
    _process_subscriptions (log, sub_state) {
        const mqtt      = this.mqtt;
        const send      = this.sub_changes;

        /* Find topics which change to a given state */
        const changes = state => sub_state.pipe(
            rx.map(v => v[1]),
            rx.filter(ch => ch[1] == state),
            rx.map(ch => ch[0]),
        );

        /* XXX None of these subscriptions can be stopped. We should
         * make ourself Unsubscribable? */
        rx.merge(changes("opening"), changes("retry")).subscribe(topic => {
            log("Subscribing to %s", topic);
            mqtt.subscribeAsync(topic)
                /* If we get an exception, return a fake broker error
                 * response. Code 0x180 is an impossible error. */
                .catch(err => {
                    log("Error subscribing to %s: %s", topic, err.message);
                    return { topic, qos: 0x180 };
                })
                /* Push the result to the sub_acks sequence for processing */
                .then(res => {
                    if (res.length != 1 || res[0].topic != topic)
                        throw new SPAppError(
                            "Bad MQTT subscribe response for %s: %o",
                            topic, res);
                    send.next([topic, res[0].qos & 0x80 ? "failed" : "open"]);
                });
        });
        changes("closing").subscribe(topic => {
            log("Unsubscribing from %s", topic);
            mqtt.unsubscribeAsync(topic)
                .then(() => send.next([topic, "closed"]));
        });
        /* We need to reconnect if we have failed subscriptions */
        changes("failed").pipe(
            /* Don't emit more than once in 5s. Emit at the end. */
            rx.auditTime(5000),
            /* Find the latest state of our subscriptions */
            rx.withLatestFrom(sub_state),
            rx.map(v => v[1][0]),
        ).subscribe(subs => {
            const failed = subs.toSeq()
                .filter((v, k) => v == "failed")
            if (failed.isEmpty()) return;
            log("Reconnecting MQTT to recover failed subscriptions");
            //process.exit(1);

            log("Failures: %o", failed.toJS());
            mqtt.once("authenticated", () => {
                /* Bug fix: MQTT.js fails to reset these flags
                 * internally on reconnection. This causes all
                 * subsequent subscribes to fail. */
                mqtt.connected = true;
                mqtt.disconnecting = false;

                const retry = subs.toSeq()
                    .filter((v, k) => v == "failed"
                        || v == "opening" || v == "retry")
                    .keySeq();
                log("Reconnected, retrying %o", retry.toJS());
                /* Restart failed subscriptions */
                retry.forEach(t => send.next([t, "retry"]));
            });
            mqtt.reconnect();
        });
    }

    subscribe (topic) {
        this.sub_changes.next([topic, "opening"]);
    }

    unsubscribe (topic) {
        this.sub_changes.next([topic, "closing"]);
    }

    /* Return a sequence following a particular topic. These sequences
     * are shared and cached so we can track the subscriptions we need. */
    watch_topic (topic) {
        const log = this.debug.log.bind(this.debug, "topic");

        if (this.topics.has(topic)) {
            log("Using cached seq for %s", topic);
            return this.topics.get(topic);
        }

        log("Starting new seq for %s", topic);
        /* This inner sequence will be shared and cached. The call to
         * rx.using does setup and teardown. */
        const inner = rx.using(
            () => {
                this.subscribe(topic);
                return new rx.Subscription(() => this.unsubscribe(topic));
            },
            () => this.packets.pipe(rx.filter(p => p.topic == topic)),
        );
        /* Share the inner sequence between everyone watching this
         * topic. This ensures we only attempt to subscribe to MQTT once
         * per topic. */
        const seq = inner.pipe(
            /* Delay 5s after we have no subscribers in case we get some
             * more. This avoids churn in our MQTT subscriptions. */
            rx.share({ resetOnRefCountZero: () => rx.timer(5000) }),
        );
        /* XXX This is a memory leak: we never clean up old topic
         * sequences. We cannot clear the cache when we unsubscribe from
         * MQTT as someone still holding onto the old sequence could
         * resubscribe. This could probably be fixed with WeakRef and
         * FinalizationRegistry. */
        this.topics.set(topic, seq);
        return seq;
    }

    /* Return a sequence of all packets from this address. types is a
     * list of Sparkplug topic types. */
    watch_address (addr, types) {
        types ??= ["BIRTH", "DEATH", "DATA"];

        const topics = types
            .map(typ => addr.topic(typ))
            .map(top => this.watch_topic(top));
        return rx.merge(...topics).pipe(
            rx.map(p => {
                const top = Topic.parse(p.topic);
                return { ...top, ...p.payload };
            }),
        );
    }

    device (opts) {
        return new SparkplugDevice(this, opts);
    }
}
