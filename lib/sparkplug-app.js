/* ACS Sparkplug App library.
 * Sparkplug App class.
 * Copyright 2023 AMRC.
 */

import rx from "rxjs";

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

        this.packets = rx.fromEvent(mqtt, "message", (t, m) => [t, m])
            .pipe(
                rx.map(([t, p]) => ({
                    topic:      Topic.parse(t), 
                    payload:    SpB.decodePayload(p),
                })));

        const wlog = this.debug.log.bind(this.debug, "watch");

        this.watch = new rx.Subject();
        const retry = new rx.Subject();
        this.watch.pipe(
            rx.flatMap(addr => rx.from(
                ["BIRTH", "DEATH", "DATA"]
                .map(t => addr.topic(t)))),
            rx.mergeWith(retry),
            rx.tap(v => wlog("%o", v)),
            /* Attempt to subscribe */
            rx.mergeMap(topic => mqtt.subscribeAsync(topic)
                .catch(e => {
                    wlog("MQTT subscribe error %s", e.message ?? e);
                    /* Return a fake error response */
                    return { topic, qos: 0x180 };
                })),
            /* Flatten arrays into the sequence */
            rx.mergeAll(),
            /* Pick out the errors */
            rx.filter(grant => grant.qos & 0x80),
            /* Debounce and buffer the failures. Connect shares our
             * upstream subscription so we can use the sequence twice.
             * debounceTime emits when we've had no new errors for 5s. */
            rx.connect(errs => {
                const finish = errs.pipe(rx.debounceTime(5000));
                return errs.pipe(rx.buffer(finish));
            }),
        ).subscribe(errs => {
            wlog("Error subscribing: %o", errs);
            wlog("Restarting MQTT connection to recover");
            mqtt.reconnect();
            mqtt.once("authenticated", () => {
                /* Bug fix: MQTT.js fails to reset these flags
                 * internally on reconnection. This causes all
                 * subsequent subscribes to fail. */
                mqtt.connected = true;
                mqtt.disconnecting = false;
                const resub = new Set(errs.map(e => e.topic));
                for (const topic of resub) {
                    wlog("Resubscribing to %s", topic);
                    /* XXX I don't think this is the right way to do this */
                    retry.next(topic);
                }
            });
        });

        return this;
    }

    /* This subscribes to MQTT when the method is called. This is
     * incorrect: our MQTT subscriptions should be driven by the
     * sequence subscriptions. */
    watch_address (addr) {
        this.watch.next(addr);
        return this.packets.pipe(
            rx.filter(m => m.topic.address.equals(addr)),
            rx.map(({topic, payload}) => ({
                type:       topic.type,
                address:    topic.address,
                ...payload,
            })));
    }

    device (opts) {
        return new SparkplugDevice(this, opts);
    }
}
