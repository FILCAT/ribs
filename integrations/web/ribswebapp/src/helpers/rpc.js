const RPC = require("rpc-websockets").Client;

class RibsRPC {
    static instance = null;
    static connectCallbacks = [];
    static disconnectCallbacks = [];

    static createInstance() {
        const rpcInstance = new RPC("ws://127.0.0.1:9010/rpc/v0"); // todo do not hardcode

        RibsRPC.connectCallbacks.forEach((callback) => {
            rpcInstance.on("open", callback);
        });

        RibsRPC.disconnectCallbacks.forEach((callback) => {
            rpcInstance.on("close", callback);
        });

        return rpcInstance;
    }

    static get() {
        if (RibsRPC.instance === null) {
            RibsRPC.instance = RibsRPC.createInstance();
            RibsRPC.autoReconnect();
        }
        return RibsRPC.instance;
    }

    static async call(method, params) {
        return await RibsRPC.get().call('RIBS.' + method, params);
    }

    static async callFil(method, params) {
        return await RibsRPC.get().call('Filecoin.' + method, params);
    }

    static onConnect(callback) {
        RibsRPC.connectCallbacks.push(callback);
        RibsRPC.get().on("open", callback);
    }

    static onDisconnect(callback) {
        RibsRPC.disconnectCallbacks.push(callback);
        RibsRPC.get().on("close", callback);
    }

    static autoReconnect() {
        RibsRPC.get().on("close", () => {
            setTimeout(() => {
                RibsRPC.instance = RibsRPC.createInstance();
                RibsRPC.autoReconnect();
            }, 5000); // Reconnect every 5 seconds
        });
    }
}

export default RibsRPC;
