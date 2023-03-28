import React, { useEffect, useState } from "react";
import RibsRPC from "../../helpers/rpc";

function RpcStatus() {
    const [status, setStatus] = useState("Connecting...");

    useEffect(() => {
        RibsRPC.onConnect(() => {
            setStatus("RPC: OK");
        });

        RibsRPC.onDisconnect(() => {
            setStatus("RPC: Disconnected");
        });
    }, []);

    return <div className="RpcStatus">{status}</div>;
}

export default RpcStatus;
