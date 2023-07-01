import './Root.css';
import { Link, Outlet, useLocation } from "react-router-dom";
import RpcStatus from "./Root/RpcStatus";

function Root() {
    const location = useLocation();

    const formatPath = () => {
        if (location.pathname === "/") {
            return "Status";
        }
        return location.pathname.replace("/", "");
    };

    return (
        <div className="Root">
            <div className="Root-head">
                <div className="Root-head-logoname">RIBS</div>
                <div className="Root-head-path">{formatPath()}</div>
                <div className="Root-head-state"><RpcStatus /></div>
            </div>
            <div className="Root-nav">
                <div className="Root-nav-item">
                    <Link to={`/`}>Status</Link>
                </div>
                <div className="Root-nav-item">
                    <Link to={`/groups`}>Groups</Link>
                </div>
                <div className="Root-nav-item">
                    <Link to={`/providers`}>Providers</Link>
                </div>
                <div className="Root-nav-item">
                    <Link to={`/content`}>Content</Link>
                </div>
            </div>
            <div className="Root-body">
                <Outlet />
            </div>
        </div>
    );
}

export default Root;