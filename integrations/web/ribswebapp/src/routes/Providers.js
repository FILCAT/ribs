import React, { useState, useEffect } from "react";
import RibsRPC from "../helpers/rpc";
import { formatBytesBinary, formatFil, epochToMonth, formatTimestamp } from "../helpers/fmt";
import "./Providers.css";
import {Link} from "react-router-dom";

function Providers() {
    const [providers, setProviders] = useState([]);

    const fetchProviders = async () => {
        try {
            const providerData = await RibsRPC.call("ReachableProviders");
            setProviders(providerData.reverse());
        } catch (error) {
            console.error("Error fetching providers:", error);
        }
    };

    useEffect(() => {
        fetchProviders();
        const intervalId = setInterval(fetchProviders, 5000);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    const calculateDealPercentage = (part, total) => {
        if(total === 0){
            return 0;
        }

        return Math.round((part / total) * 100);
    };

    return (
        <div className="Providers">
            <h2>Providers</h2>
            <table className="providers-table">
                <thead>
                <tr>
                    <th></th>
                    <th>Address</th>
                    <th>
                        <div>Piece Sizes</div>
                        <div>Price</div>
                    </th>
                    <th>Features</th>
                    <th>Started</th>
                    <th>Rejected</th>
                    <th>Failed</th>
                    <th>Sealed</th>
                    <th>Retriev</th>
                    <th>Last Deal Start</th>
                </tr>
                </thead>
                <tbody>
                {providers.map((provider, i) => (
                    <tr key={provider.ID}>
                        <td className="providers-ask">{i+1}.</td>
                        <td><Link to={`/provider/f0${provider.ID}`}>f0{provider.ID}</Link></td>
                        <td className="providers-ask">
                            <div>{`${formatBytesBinary(provider.AskMinPieceSize)} to ${formatBytesBinary(provider.AskMaxPieceSize)}`}</div>
                            <div>{`${formatFil(provider.AskPrice * epochToMonth)} (${formatFil(provider.AskVerifiedPrice * epochToMonth)})`}</div>
                        </td>
                        <td>
                            {`${provider.BoosterHttp ? "http " : ""} ${provider.BoosterBitswap ? "bitswap" : ""}`.trim()}
                        </td>
                        <td>{provider.DealStarted}</td>
                        <td>
                            {provider.DealRejected}
                            <div className="progress-bar thin-bar">
                                <div className="progress-bar__fill progress-bar__fill-red" style={{ width: `${calculateDealPercentage(provider.DealRejected, provider.DealStarted)}%` }}></div>
                            </div>
                        </td>
                        <td>
                            {provider.DealFail}
                            <div className="progress-bar thin-bar">
                                <div className="progress-bar__fill progress-bar__fill-red" style={{ width: `${calculateDealPercentage(provider.DealFail, provider.DealStarted)}%` }}></div>
                            </div>
                        </td>
                        <td>
                            {provider.DealSuccess}
                            <div className="progress-bar thin-bar">
                                <div className="progress-bar__fill" style={{ width: `${calculateDealPercentage(provider.DealSuccess, provider.DealStarted)}%` }}></div>
                            </div>
                        </td>
                        <td>
                            {provider.RetrievDeals} ({calculateDealPercentage(provider.RetrievDeals, provider.UnretrievDeals+provider.RetrievDeals)}%)
                            <div className="progress-bar thin-bar">
                                <div className="progress-bar__fill" style={{ width: `${calculateDealPercentage(provider.RetrievDeals, provider.UnretrievDeals+provider.RetrievDeals)}%` }}></div>
                            </div>
                        </td>
                        <td className={"providers-ask" + (((new Date()) - provider.MostRecentDealStart*1000) > 80000000 ? " providers-nodeal-longtime" : "") }>
                            {provider.MostRecentDealStart === 0 ? "Never" : formatTimestamp(provider.MostRecentDealStart)}
                        </td>
                    </tr>
                ))}
                </tbody>
            </table>
        </div>
    );
}

export default Providers;
