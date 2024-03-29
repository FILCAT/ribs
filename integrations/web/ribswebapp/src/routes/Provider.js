import React, { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';
import RibsRPC from "../helpers/rpc";
import {
    formatBytesBinary,
    formatFil,
    epochToMonth,
    epochToDate,
    epochToDuration,
    formatTimestamp
} from "../helpers/fmt";
import "./Status.css";
import "./Provider.css";

function Provider() {
    const [provider, setProvider] = useState(null);
    const [headHeight, setHeadHeight] = useState(0);
    const { providerID } = useParams();

    const fetchProvider = async () => {
        try {
            let prov = providerID.slice(2, providerID.length)
            prov = parseInt(prov)

            const provideData = await RibsRPC.call("ProviderInfo", [prov]);
            setProvider(provideData);

            const head = await RibsRPC.callFil("ChainHead");
            setHeadHeight(head.Height);
        } catch (error) {
            console.error("Error fetching providers:", error);
        }
    };

    useEffect(() => {
        fetchProvider();
        const intervalId = setInterval(fetchProvider, 2500);

        return () => {
            clearInterval(intervalId);
        };
    }, []);

    return (
        <div className="Provider">
            {provider && (
                <>
                    <h2>Provider {providerID}</h2>

                    <table className="compact-table provider-meta-table">
                        <tbody>
                        <tr>
                            <td>Ping OK</td>
                            <td>{provider.Meta.PingOk ? "Yes" : "No"}</td>
                        </tr>
                        <tr>
                            <td>Boost Deals</td>
                            <td>{provider.Meta.BoostDeals ? "Yes" : "No"}</td>
                        </tr>
                        <tr>
                            <td>Booster HTTP</td>
                            <td>{provider.Meta.BoosterHttp ? "Yes" : "No"}</td>
                        </tr>
                        <tr>
                            <td>Booster Bitswap</td>
                            <td>{provider.Meta.BoosterBitswap ? "Yes" : "No"}</td>
                        </tr>
                        <tr>
                            <td>Ask Price</td>
                            <td>{formatFil(provider.Meta.AskPrice)}</td>
                        </tr>
                        <tr>
                            <td>Ask Verified Price</td>
                            <td>{formatFil(provider.Meta.AskVerifiedPrice)}</td>
                        </tr>
                        <tr>
                            <td>Ask Min Piece Size</td>
                            <td>{formatBytesBinary(provider.Meta.AskMinPieceSize)}</td>
                        </tr>
                        <tr>
                            <td>Ask Max Piece Size</td>
                            <td>{formatBytesBinary(provider.Meta.AskMaxPieceSize)}</td>
                        </tr>
                        <tr>
                            <td>Indexed Success</td>
                            <td>{provider.Meta.IndexedSuccess}</td>
                        </tr>
                        <tr>
                            <td>Indexed Fail</td>
                            <td>{provider.Meta.IndexedFail}</td>
                        </tr>
                        <tr>
                            <td>Deal Started</td>
                            <td>{provider.Meta.DealStarted}</td>
                        </tr>
                        <tr>
                            <td>Deal Success</td>
                            <td>{provider.Meta.DealSuccess}</td>
                        </tr>
                        <tr>
                            <td>Deal Fail</td>
                            <td>{provider.Meta.DealFail}</td>
                        </tr>
                        <tr>
                            <td>Deal Rejected</td>
                            <td>{provider.Meta.DealRejected}</td>
                        </tr>
                        <tr>
                            <td>Retr Probe Success</td>
                            <td>{provider.Meta.RetrProbeSuccess}</td>
                        </tr>
                        <tr>
                            <td>Retr Probe Fail</td>
                            <td>{provider.Meta.RetrProbeFail}</td>
                        </tr>
                        <tr>
                            <td>Retr Probe Blocks</td>
                            <td>{provider.Meta.RetrProbeBlocks}</td>
                        </tr>
                        <tr>
                            <td>Retr Probe Bytes</td>
                            <td>{formatBytesBinary(provider.Meta.RetrProbeBytes)}</td>
                        </tr>
                        <tr>
                            <td>Links</td>
                            <td>
                                <a href={`https://filfox.info/en/address/${providerID}`} target="_blank">FilFox</a>
                            </td>
                        </tr>

                        </tbody>
                    </table>

                    <h4>Recent Deals</h4>

                    <table className="compact-table providers-table">
                        <thead>
                        <tr>
                            <th>UUID</th>
                            <th>Status</th>
                            <th>Timing</th>
                            <th>Error</th>
                            <th>Deal ID</th>
                            <th>Transferred</th>
                            <th>Publish CID</th>
                        </tr>
                        </thead>
                        <tbody>
                        {provider.RecentDeals && provider.RecentDeals.map((deal) => (
                            <tr key={deal.UUID} style={{background: (deal.Rejected ? '#fff7cc' : (deal.Failed ? '#f5c4c4' : (deal.Sealed ? '#caffcd' : '#f2f3ff')))}}>

                                <td><abbr title={deal.UUID}>{deal.UUID.substring(0, 8)}... </abbr></td>
                                <td>{deal.Status}</td>
                                <td className="provider-deals-nowrap">
                                    <div>
                                        <div>Proposed: <b>{formatTimestamp(deal.StartTime)}</b></div>
                                        <div>Start: <b>{epochToDate(deal.StartEpoch)}</b>, {epochToDuration(deal.StartEpoch-headHeight)}</div>
                                        <div>End: <b>{epochToDate(deal.EndEpoch)}</b>, {epochToDuration(deal.EndEpoch-headHeight)}</div>
                                    </div>
                                </td>
                                <td className="provider-deals-error-col">{deal.Error && <pre>{deal.Error}</pre>}</td>
                                <td>{deal.DealID && <a href={`https://filfox.info/en/deal/${deal.DealID}`} target="_blank" rel="noopener noreferrer">{deal.DealID}</a> || <></>}</td>
                                <td>
                                    {!(!deal.BytesRecv || deal.Status == 'Transferred' || deal.PubCid) && (
                                        <div className="prov-progress-container">
                                            <div className="prov-text-with-progress">
                                                {formatBytesBinary(deal.BytesRecv)}/{formatBytesBinary(deal.TxSize)}
                                            </div>
                                            <div className="progress-bar thin-bar">
                                                <div
                                                    className="progress-bar__fill"
                                                    style={{width: `${(deal.BytesRecv / deal.TxSize) * 100}%`}}
                                                ></div>
                                            </div>
                                        </div>
                                    ) || <></>}
                                </td>
                                <td>{deal.PubCid && <a href={`https://filfox.info/en/message/${deal.PubCid}`} target="_blank" rel="noopener noreferrer">bafy..{deal.PubCid.substr(-16)}</a>}</td>
                            </tr>
                        ))}
                        </tbody>
                    </table>
                </>
            )}
        </div>
    );
}

export default Provider;