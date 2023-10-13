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
        const maxRetries = 5;
        let delay = 1000; // 1 second in milliseconds

        for (let i = 0; i < maxRetries; i++) {
            try {
                return await RibsRPC.get().call('RIBS.' + method, params);
            } catch (error) {
                if (i === maxRetries - 1) {
                    throw error; // Re-throw the error if we've reached the maximum number of retries
                }
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
    }

    static async callFil(method, params) {
        const maxRetries = 5;
        let delay = 1000; // 1 second in milliseconds

        for (let i = 0; i < maxRetries; i++) {
            try {
                return await RibsRPC.get().call('Filecoin.' + method, params);
            } catch (error) {
                if (i === maxRetries - 1) {
                    throw error; // Re-throw the error if we've reached the maximum number of retries
                }
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
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
