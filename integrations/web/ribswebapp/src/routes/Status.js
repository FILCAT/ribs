import './Status.css';
import React, { useState, useEffect } from "react";
import RibsRPC from "../helpers/rpc";
import { formatBytesBinary } from "../helpers/fmt";

function WalletInfoTile({ walletInfo }) {
    const truncateAddress = (address) => {
        const head = address.slice(0, 10);
        const tail = address.slice(-10);
        return `${head}...${tail}`;
    };

    return (
        <div>
            <h2>Wallet Info</h2>
            {walletInfo && (
                <table className="compact-table">
                    <tbody>
                    <tr>
                        <td>Address</td>
                        <td>{truncateAddress(walletInfo.Addr)}</td>
                    </tr>
                    <tr>
                        <td>Balance:</td>
                        <td>{walletInfo.Balance}</td>
                    </tr>
                    <tr>
                        <td>Market Balance:</td>
                        <td>{walletInfo.MarketBalance}</td>
                    </tr>
                    <tr>
                        <td>Market Locked:</td>
                        <td>{walletInfo.MarketLocked}</td>
                    </tr>
                    <tr>
                        <td>DataCap:</td>
                        <td className="important-metric">XX TiB</td>
                    </tr>
                    </tbody>
                </table>
            )}
        </div>
    );
}

function GroupsTile({ groups }) {
    return (
        <div>
            <h2>Groups: {groups.length}</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Total data size:</td>
                    <td className="important-metric">XX TiB</td>
                </tr>
                <tr>
                    <td>Total block count:</td>
                    <td>14.24G</td>
                </tr>
                <tr>
                    <td>Local size:</td>
                    <td>XX TiB</td>
                </tr>
                <tr>
                    <td>Offloaded size:</td>
                    <td>XX TiB</td>
                </tr>
                <tr>
                    <td>Open (RO):</td>
                    <td>3</td>
                </tr>
                <tr>
                    <td>Open (RW):</td>
                    <td>1</td>
                </tr>
                </tbody>
            </table>
        </div>
    );
}

function TopIndexTile({ groups }) {
    return (
        <div>
            <h2>Top Index</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Entries:</td>
                    <td className="important-metric">123 423K</td>
                </tr>
                <tr>
                    <td>Read rate:</td>
                    <td>1 234/s</td>
                </tr>
                <tr>
                    <td>Write rate:</td>
                    <td>12/s</td>
                </tr>
                </tbody>
            </table>
        </div>
    );
}

function DealsTile({}) {
    return (
        <div>
            <h2>Deals: 123</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Total data size:</td>
                    <td className="important-metric">XX TiB</td>
                </tr>
                <tr>
                    <td>Avg. deals per group:</td>
                    <td>4.9/5</td>
                </tr>
                <tr>
                    <td>Deals in progress:</td>
                    <td>12</td>
                </tr>
                <tr>
                    <td>Deals done:</td>
                    <td>111</td>
                </tr>
                <tr>
                    <td>Deals failed:</td>
                    <td>0</td>
                </tr>
                </tbody>
            </table>
        </div>
    );
}

function ProvidersTile({ reachableProviders }) {
    return (
        <div>
            <h2>Providers</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Reachable Providers:</td>
                    <td>{reachableProviders.length}</td>
                </tr>
                <tr>
                    <td>With booster-bitswap:</td>
                    <td>{reachableProviders.filter(p => p.BoosterBitswap).length}</td>
                </tr>
                <tr>
                    <td>With booster-http:</td>
                    <td>{reachableProviders.filter(p => p.BoosterHttp).length}</td>
                </tr>
                <tr>
                    <td>With attempted deals:</td>
                    <td>{reachableProviders.filter(p => p.DealStarted).length}</td>
                </tr>
                <tr>
                    <td>With successful deals:</td>
                    <td>{reachableProviders.filter(p => p.DealSuccess).length}</td>
                </tr>
                <tr>
                    <td>With all rejected deals:</td>
                    <td>{reachableProviders.filter(p => p.DealRejected).length}</td>
                </tr>
                </tbody>
            </table>
        </div>
    );
}

function WorkersTile({ groups }) {
    return (
        <div>
            <h2>Workers: 1</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Busy workers:</td>
                    <td className="important-metric">1/1</td>
                </tr>
                <tr>
                    <td>Generating BSST:</td>
                    <td>0/1</td>
                </tr>
                <tr>
                    <td>Generating VCAR:</td>
                    <td>0/1</td>
                </tr>
                <tr>
                    <td>Generating CommP:</td>
                    <td>0/1</td>
                </tr>
                <tr>
                    <td>Making deals:</td>
                    <td>1/1</td>
                </tr>
                </tbody>
            </table>
        </div>
    );
}

function CarUploadStatsTile({ carUploadStats }) {
    return (
        <div>
            <h2>Car Upload Stats</h2>
            <table className="compact-table">
                <thead>
                <tr>
                    <th style={{ width: '30%' }}>Group</th>
                    <th style={{ width: '30%' }}>Reqs</th>
                    <th style={{ width: '40%' }}>Rate</th>
                </tr>
                </thead>
                <tbody>
                {Object.entries(carUploadStats).map(([groupKey, uploadStats]) => (
                    <tr key={groupKey}>
                        <td>Group {groupKey}</td>
                        <td>{uploadStats.ActiveRequests}</td>
                        <td>{formatBytesBinary(uploadStats.Last250MsUploadBytes * 4)}/s</td>
                    </tr>
                ))}
                </tbody>
            </table>
        </div>
    );
}

function CrawlStateTile() {
    return (
        <div>
            <h2>Crawl State</h2>
            <b>querying providers</b>
            <div>
                At <b>640</b>; Reachable <b>62</b>; Boost/BBitswap/BHttp <b>22/1/1</b>
            </div>
        </div>
    );
}

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
                <GroupsTile groups={groups} />
                <TopIndexTile />
                <DealsTile />
                <WorkersTile groups={groups} />
                <ProvidersTile reachableProviders={reachableProviders} />
                <CarUploadStatsTile carUploadStats={carUploadStats} />
                <CrawlStateTile />
                <WalletInfoTile walletInfo={walletInfo} />
            </div>
        </div>
    );
}

export default Status;
