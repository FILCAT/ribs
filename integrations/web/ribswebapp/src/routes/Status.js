import './Status.css';
import React, { useState, useEffect } from "react";
import RibsRPC from "../helpers/rpc";

function Status() {
    const [walletInfo, setWalletInfo] = useState(null);
    const [groups, setGroups] = useState([]);
    const [crawlState, setCrawlState] = useState("");
    const [carUploadStats, setCarUploadStats] = useState({});
    const [reachableProviders, setReachableProviders] = useState([]);

    const fetchStatus = async () => {
        try {
            const walletInfo = await RibsRPC.call("WalletInfo");
            const groups = await RibsRPC.call("Groups");
            const crawlState = await RibsRPC.call("CrawlState");
            const carUploadStats = await RibsRPC.call("CarUploadStats");
            const reachableProviders = await RibsRPC.call("ReachableProviders");

            setWalletInfo(walletInfo);
            setGroups(groups);
            setCrawlState(crawlState);
            setCarUploadStats(carUploadStats);
            setReachableProviders(reachableProviders);
        } catch (error) {
            console.error("Error fetching status:", error);
        }
    };

    useEffect(() => {
        fetchStatus();
        const intervalId = setInterval(fetchStatus, 1000);

        return () => {
            clearInterval(intervalId);
        };
    }, []);


    return (
        <div className="Status">
            <div className="status-grid">
                <div>
                    <h2>Wallet Info</h2>
                    {walletInfo && (
                        <div>
                            <p>Address: {walletInfo.Addr}</p>
                            <p>Balance: {walletInfo.Balance}</p>
                            <p>Market Balance: {walletInfo.MarketBalance}</p>
                            <p>Market Locked: {walletInfo.MarketLocked}</p>
                        </div>
                    )}
                </div>
                <div>
                    <h2>Groups: {groups.length}</h2>
                </div>
                <div>
                    <h2>Crawl State</h2>
                    <b>querying providers</b>
                    <div>
                        At <b>640</b>; Reachable <b>62</b>; Boost/BBitswap/BHttp <b>22/1/1</b>
                    </div>
                </div>
                <div>
                    <h2>Car Upload Stats</h2>
                    <ul>
                        {Object.entries(carUploadStats).map(([groupKey, uploadStats]) => (
                            <li key={groupKey}>
                                Group: {groupKey} - Active Requests: {uploadStats.ActiveRequests} - Last 250ms Upload Bytes:{" "}
                                {uploadStats.Last250MsUploadBytes}
                            </li>
                        ))}
                    </ul>
                </div>
                <div>
                    <h2>Providers</h2>
                    <p>Reachable Providers: {reachableProviders.length}</p>
                    <p>With booster-bitswap: {reachableProviders.filter(p => p.BoosterBitswap).length}</p>
                    <p>With booster-http: {reachableProviders.filter(p => p.BoosterHttp).length}</p>
                    <p>With attempted deals: {reachableProviders.filter(p => p.DealStarted).length}</p>
                    <p>With successful deals: {reachableProviders.filter(p => p.DealSuccess).length}</p>
                    <p>With all rejected deals: {reachableProviders.filter(p => p.DealRejected).length}</p>
                </div>
            </div>
        </div>
    );
}

export default Status;