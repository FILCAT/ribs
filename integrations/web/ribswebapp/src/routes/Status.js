import './Status.css';
import React, { useState, useEffect, useRef } from "react";
import RibsRPC from "../helpers/rpc";
import {formatBytesBinary, formatNum, formatNum6, calcEMA} from "../helpers/fmt";

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
            <h2>Block Groups: {groups.length}</h2>
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

function TopIndexTile() {
    const [indexStats, setTopIndexStats] = useState({});
    const prevStatsRef = useRef({});
    const readRateEMARef = useRef(0);
    const writeRateEMARef = useRef(0);
    const smoothingFactor = 1 / 10;

    const fetchStatus = async () => {
        try {
            const topIndexStats = await RibsRPC.call("TopIndexStats");

            const prevStats = prevStatsRef.current;
            const reads = topIndexStats.Reads;
            const writes = topIndexStats.Writes;

            if (prevStats.Reads !== undefined && prevStats.Writes !== undefined) {
                const readRate = reads - prevStats.Reads;
                const writeRate = writes - prevStats.Writes;

                readRateEMARef.current = calcEMA(
                    readRate,
                    readRateEMARef.current,
                    smoothingFactor
                );
                writeRateEMARef.current = calcEMA(
                    writeRate,
                    writeRateEMARef.current,
                    smoothingFactor
                );
            }

            setTopIndexStats({
                ...topIndexStats,
                ReadRate: Math.round(readRateEMARef.current),
                WriteRate: Math.round(writeRateEMARef.current),
            });

            prevStatsRef.current = { Reads: reads, Writes: writes };
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
        <div>
            <h2>Top Index</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Entries:</td>
                    <td className="important-metric">{formatNum6(indexStats.Entries)}</td>
                </tr>
                <tr>
                    <td>Read rate:</td>
                    <td>{indexStats.ReadRate}/s</td>
                </tr>
                <tr>
                    <td>Write rate:</td>
                    <td>{indexStats.WriteRate}/s</td>
                </tr>
                </tbody>
            </table>
        </div>
    );
}


function DealsTile({ dealSummary }) {
    return (
        <div>
            <h2>Deals: {dealSummary.InProgress + dealSummary.Done}</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Total data size:</td>
                    <td className="important-metric">{formatBytesBinary(dealSummary.TotalDataSize)}</td>
                </tr>
                <tr>
                    <td>Total deal size:</td>
                    <td className="important-metric">{formatBytesBinary(dealSummary.TotalDealSize)}</td>
                </tr>
                <tr>
                    <td>Deals in progress:</td>
                    <td>{dealSummary.InProgress}</td>
                </tr>
                <tr>
                    <td>Deals done:</td>
                    <td>{dealSummary.Done}</td>
                </tr>
                <tr>
                    <td>Deals failed:</td>
                    <td>{dealSummary.Failed}</td>
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


function CrawlStateTile({ crawlState }) {
    const progressBarPercentage = crawlState.State === "querying providers" ? (crawlState.At / crawlState.Total) * 100 : 0;
    const showAt = ["listing market participants", "querying providers"].includes(crawlState.State);

    return (
        <div className="CrawlStateTile">
            <h2>Crawl State</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td><b>State:</b></td>
                    <td>{crawlState.State}</td>
                </tr>
                {showAt && (
                    <tr>
                        <td><b>At:</b></td>
                        <td>{crawlState.At}</td>
                    </tr>
                )}
                {crawlState.State === "querying providers" && (
                    <>
                        <tr>
                            <td><b>Progress:</b></td>
                            <td>{progressBarPercentage.toFixed(2)}%</td>
                        </tr>
                        <tr>
                            <td colSpan={2}>
                                <div className="progress-bar">
                                    <div className="progress-bar__fill" style={{ width: `${progressBarPercentage}%` }}></div>
                                </div>
                            </td>
                        </tr>
                        <tr>
                            <td><b>Reachable:</b></td>
                            <td>{crawlState.Reachable}</td>
                        </tr>
                        <tr>
                            <td><b>Total:</b></td>
                            <td>{crawlState.Total}</td>
                        </tr>
                        <tr>
                            <td><b>Boost:</b></td>
                            <td>{crawlState.Boost}</td>
                        </tr>
                        <tr>
                            <td><b>BBswap:</b></td>
                            <td>{crawlState.BBswap}</td>
                        </tr>
                        <tr>
                            <td><b>BHttp:</b></td>
                            <td>{crawlState.BHttp}</td>
                        </tr>
                    </>
                )}
                </tbody>
            </table>
        </div>
    );
}

function IoStats() {
    const [groupIOStats, setGroupIOStats] = useState({});
    const prevStatsRef = useRef({});
    const readBlocksEMARef = useRef(0);
    const writeBlocksEMARef = useRef(0);
    const readBytesEMARef = useRef(0);
    const writeBytesEMARef = useRef(0);
    const smoothingFactor = 1 / 10;

    const fetchStatus = async () => {
        try {
            const ioStats = await RibsRPC.call("GroupIOStats");

            const prevStats = prevStatsRef.current;
            const readBlocks = ioStats.ReadBlocks;
            const writeBlocks = ioStats.WriteBlocks;
            const readBytes = ioStats.ReadBytes;
            const writeBytes = ioStats.WriteBytes;

            if (prevStats.ReadBlocks !== undefined && prevStats.WriteBlocks !== undefined) {
                const readBlocksRate = readBlocks - prevStats.ReadBlocks;
                const writeBlocksRate = writeBlocks - prevStats.WriteBlocks;
                const readBytesRate = readBytes - prevStats.ReadBytes;
                const writeBytesRate = writeBytes - prevStats.WriteBytes;

                readBlocksEMARef.current = calcEMA(
                    readBlocksRate,
                    readBlocksEMARef.current,
                    smoothingFactor
                );
                writeBlocksEMARef.current = calcEMA(
                    writeBlocksRate,
                    writeBlocksEMARef.current,
                    smoothingFactor
                );
                readBytesEMARef.current = calcEMA(
                    readBytesRate,
                    readBytesEMARef.current,
                    smoothingFactor
                );
                writeBytesEMARef.current = calcEMA(
                    writeBytesRate,
                    writeBytesEMARef.current,
                    smoothingFactor
                );
            }

            setGroupIOStats({
                ...ioStats,
                ReadBlocksRate: Math.round(readBlocksEMARef.current),
                WriteBlocksRate: Math.round(writeBlocksEMARef.current),
                ReadBytesRate: Math.round(readBytesEMARef.current),
                WriteBytesRate: Math.round(writeBytesEMARef.current),
            });

            prevStatsRef.current = { ReadBlocks: readBlocks, WriteBlocks: writeBlocks, ReadBytes: readBytes, WriteBytes: writeBytes };
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
        <div>
            <h2>IO Stats</h2>
            <table className="compact-table">
                <tbody>
                <tr>
                    <td>Read Rate:</td>
                    <td>{formatNum(groupIOStats.ReadBlocksRate)} Blk/s</td>
                </tr>
                <tr>
                    <td>Read Bytes:</td>
                    <td>{formatBytesBinary(groupIOStats.ReadBytesRate)}/s</td>
                </tr>
                <tr>
                    <td>Write Rate:</td>
                    <td>{formatNum(groupIOStats.WriteBlocksRate)} Blk/s</td>
                </tr>
                <tr>
                    <td>Write Bytes:</td>
                    <td>{formatBytesBinary(groupIOStats.WriteBytesRate)}/s</td>
                </tr>
                </tbody>
            </table>
        </div>
    );
}

function Status() {
    const [walletInfo, setWalletInfo] = useState(null);
    const [groups, setGroups] = useState([]);
    const [crawlState, setCrawlState] = useState("");
    const [carUploadStats, setCarUploadStats] = useState({});
    const [reachableProviders, setReachableProviders] = useState([]);
    const [dealSummary, setDealSummary] = useState({});

    const fetchStatus = async () => {
        try {
            const walletInfo = await RibsRPC.call("WalletInfo");
            const groups = await RibsRPC.call("Groups");
            const crawlState = await RibsRPC.call("CrawlState");
            const carUploadStats = await RibsRPC.call("CarUploadStats");
            const reachableProviders = await RibsRPC.call("ReachableProviders");
            const dealSummary = await RibsRPC.call("DealSummary");
            const topIndexStats = await RibsRPC.call("TopIndexStats");

            setWalletInfo(walletInfo);
            setGroups(groups);
            setCrawlState(crawlState);
            setCarUploadStats(carUploadStats);
            setReachableProviders(reachableProviders);
            setDealSummary(dealSummary);
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
                <IoStats />
                <TopIndexTile />
                <DealsTile dealSummary={dealSummary} />
                <WorkersTile groups={groups} />
                <ProvidersTile reachableProviders={reachableProviders} />
                <CarUploadStatsTile carUploadStats={carUploadStats} />
                <CrawlStateTile crawlState={crawlState} />
                <WalletInfoTile walletInfo={walletInfo} />
            </div>
        </div>
    );
}

export default Status;
