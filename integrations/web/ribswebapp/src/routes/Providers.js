import React, { useState, useEffect } from "react";
import RibsRPC from "../helpers/rpc";
import { formatBytesBinary, formatFil, epochToMonth } from "../helpers/fmt";
import "./Providers.css";

function Providers() {
    const [providers, setProviders] = useState([]);

    const fetchProviders = async () => {
        try {
            const providerData = await RibsRPC.call("ReachableProviders");
            setProviders(providerData);
        } catch (error) {
            console.error("Error fetching providers:", error);
        }
    };

    useEffect(() => {
        fetchProviders();
        const intervalId = setInterval(fetchProviders, 500);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    return (
        <div className="Providers">
            <h2>Providers</h2>
            <table className="providers-table">
                <thead>
                <tr>
                    <th>Address</th>
                    <th>Piece Sizes</th>
                    <th>Price</th>
                    <th>Features</th>
                    <th>Stored</th>
                    <th>Failed</th>
                    <th>Rejected</th>
                </tr>
                </thead>
                <tbody>
                {providers.map((provider) => (
                    <tr key={provider.ID}>
                        <td>f0{provider.ID}</td>
                        <td>
                            {formatBytesBinary(provider.AskMinPieceSize)} to{" "}
                            {formatBytesBinary(provider.AskMaxPieceSize)}
                        </td>
                        <td>
                            {formatFil(provider.AskPrice * epochToMonth)} (
                            {formatFil(provider.AskVerifiedPrice * epochToMonth)})
                        </td>
                        <td>
                            {provider.PingOk ? "online" : "offline"}{" "}
                            {provider.BoostDeals ? "boost " : ""}{" "}
                            {provider.BoosterHttp ? "http " : ""}{" "}
                            {provider.BoosterBitswap ? "bitswap " : ""}
                        </td>
                        <td>
                            {provider.DealStarted === 0 ? (
                                <span className="stat-gray">
                    {provider.DealSuccess} /{" "}
                                    {provider.DealStarted -
                                        provider.DealFail -
                                        provider.DealRejected}
                  </span>
                            ) : (
                                <span>
                    {provider.DealSuccess} /{" "}
                                    {provider.DealStarted -
                                        provider.DealFail -
                                        provider.DealRejected}
                  </span>
                            )}
                        </td>
                        <td>
                            {provider.DealFail === 0 ? (
                                <span className="stat-gray">{provider.DealFail}</span>
                            ) : (
                                <span>{provider.DealFail}</span>
                            )}
                        </td>
                        <td>
                            {provider.DealRejected === 0 ? (
                                <span className="stat-gray">{provider.DealRejected}</span>
                            ) : (
                                <span>{provider.DealRejected}</span>
                            )}
                        </td>
                    </tr>
                ))}
                </tbody>
            </table>
        </div>
    );
}

export default Providers;
