/* ACS Sparkplug App library.
 * Sparkplug App class.
 * Copyright 2023 AMRC.
 */

import rx from "rxjs";

import { SpB, Topic } from "@amrc-factoryplus/utilities";

import { SPAppError } from "./spapp-error.js";

class SparkplugDevice {
    constructor (app, opts) {
        this.app = app;

        if (opts.device) {
            this.device = opts.device;
        }
        else if (opts.address) {
            this.address = opts.address;
        }
        else {
            throw new SPAppError("Must supply device or address to constructor");
        }
    }

    async init () {
        const { fplus, mqtt } = this.app;

        const addr = this.address ??= await fplus.Directory.get_device_address(this.device);
        if (!addr)
            throw new SPAppError(`Can't resolve device ${this.device}`);
        this.app.debug.log("device", "Watching %s", addr);

        this.packets = this.app.watch_address(addr);

        return this;
    }

    metric (tag) {
        return this.packets.pipe(
            rx.mergeMap(p => rx.from(p.metrics)),
            rx.filter(m => m.name == tag),
            rx.map(m => m.value));
    }
}

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
        this.packets.subscribe(p => this.debug.log("packet", "%o", p));

        this.watch = new rx.Subject();
        this.watch.pipe(
            rx.flatMap(addr => rx.from(["BIRTH", "DEATH", "DATA"])),
            rx.map(t => addr.topic(t)),
            rx.tap(v => this.debug.log("watch", "%o", v)))
        .subscribe(topic => mqtt.subscribe(topic));

        return this;
    }

    watch_address (addr) {
        this.watch.next(addr);
        return this.packets.pipe(
            rx.filter(m => m.topic.address.equals(addr)),
            rx.map(({topic, payload}) => ({type: topic.type, ...payload})));
    }

    async device (opts) {
        const dev = new SparkplugDevice(this, opts);
        return await dev.init();
    }
}
