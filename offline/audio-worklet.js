class PcmCaptureProcessor extends AudioWorkletProcessor {
    constructor() {
        super();
        this.frameSize = 480;
        this.frame = new Int16Array(this.frameSize);
        this.offset = 0;
    }

    process(inputs, outputs) {
        const output = outputs[0];
        if (output) {
            for (const channel of output) channel.fill(0);
        }

        const input = inputs[0];
        if (!input || input.length === 0 || input[0].length === 0) return true;
        const frameCount = input[0].length;
        for (let i = 0; i < frameCount; i++) {
            let sample = 0;
            for (const channel of input) sample += channel[i] || 0;
            sample /= input.length;
            sample = Math.max(-1, Math.min(1, sample));
            this.frame[this.offset++] = sample < 0 ? sample * 0x8000 : sample * 0x7fff;
            if (this.offset === this.frameSize) {
                const frame = this.frame;
                this.port.postMessage(frame.buffer, [frame.buffer]);
                this.frame = new Int16Array(this.frameSize);
                this.offset = 0;
            }
        }
        return true;
    }
}

class PcmPlaybackProcessor extends AudioWorkletProcessor {
    constructor() {
        super();
        this.streams = new Map();
        this.frame = 0;
        this.port.onmessage = ({ data }) => {
            if (!data || typeof data.streamId !== "string" ||
                !(data.samples instanceof ArrayBuffer) || data.samples.byteLength % 2 !== 0) return;
            let stream = this.streams.get(data.streamId);
            if (!stream) {
                stream = { queue: [], queuedSamples: 0, current: null, offset: 0, started: false, lastPacket: this.frame };
                this.streams.set(data.streamId, stream);
            }
            const samples = new Int16Array(data.samples);
            stream.queue.push(samples);
            stream.queuedSamples += samples.length;
            stream.lastPacket = this.frame;
            while (stream.queuedSamples > sampleRate / 10 && stream.queue.length > 1) {
                const discarded = stream.queue.shift();
                stream.queuedSamples -= discarded.length;
            }
        };
    }

    process(inputs, outputs) {
        const output = outputs[0] && outputs[0][0];
        if (!output) return true;
        output.fill(0);
        let activeStreams = 0;

        for (const [streamId, stream] of this.streams) {
            if (!stream.started && stream.queuedSamples < sampleRate * 0.02) continue;
            stream.started = true;
            let hasSamples = false;
            for (let i = 0; i < output.length; i++) {
                while (!stream.current || stream.offset >= stream.current.length) {
                    stream.current = stream.queue.shift() || null;
                    stream.offset = 0;
                    if (!stream.current) {
                        stream.started = false;
                        break;
                    }
                    stream.queuedSamples -= stream.current.length;
                }
                if (!stream.current) break;
                output[i] += stream.current[stream.offset++] / 0x8000;
                hasSamples = true;
            }
            if (hasSamples) activeStreams++;
            if (!stream.started && this.frame - stream.lastPacket > sampleRate * 5) {
                this.streams.delete(streamId);
            }
        }

        if (activeStreams > 1) {
            for (let i = 0; i < output.length; i++) output[i] /= activeStreams;
        }
        for (let i = 0; i < output.length; i++) {
            output[i] = Math.max(-1, Math.min(1, output[i]));
        }
        this.frame += output.length;
        return true;
    }
}

registerProcessor("pcm-capture", PcmCaptureProcessor);
registerProcessor("pcm-playback", PcmPlaybackProcessor);
