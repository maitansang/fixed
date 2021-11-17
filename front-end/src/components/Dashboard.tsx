import React,{useState} from "react";
import { getCurrentUser } from "../services/auth.service";
import ExecComponent from "./ExecComponent";

const Dashboard: React.FC = () => {
  const currentUser = getCurrentUser();
  const [runScript, setRunScript] = useState('');

  const buttonHandler = (text : any) => {
    setRunScript(text);
  };
  return (
    <div className="container">
      <div className="right-menu-container">
        <div className="mm-item" onClick={() => buttonHandler("dashboard")} >
          <a href="">
            <span className="">Dashboard</span>
          </a>
        </div>
        <div className="mm-item" onClick={() => buttonHandler("dashboard")}>
          <a href="">
            <span className="">Aggreagates</span>
          </a>
        </div>
        <div className="mm-item" onClick={() => buttonHandler("dashboard")}>
          <a href="">
            <span className="">Average Volume</span>
          </a>
        </div>
        <div className="mm-item" onClick={() => buttonHandler("dashboard")}>
          <a href="">
            <span className="">Trades</span>
          </a>
        </div>
        <div className="mm-item" onClick={() => buttonHandler("dashboard")}>
          <a href="">
            <span className="">Breakouthis</span>
          </a>
        </div>
        <div className="mm-item" onClick={() => buttonHandler("dashboard")}>
          <a href="">
            <span className="">Changepct</span>
          </a>
        </div>
        <div className="mm-item" onClick={() => buttonHandler("dashboard")}>
          <a href="">
            <span className="">Changepctall</span>
          </a>
        </div>
        <div className="mm-item" onClick={() => buttonHandler("dashboard")}>
          <a href="">
            <span className="">Lob</span>
          </a>
        </div>
        <div className="mm-item" onClick={() => buttonHandler("dashboard")}>
          <a href="">
            <span className="">Lov</span>
          </a>
        </div>
        <div className="mm-item" onClick={() => buttonHandler("dashboard")}>
          <a href="">
            <span className="">Pattern Features</span>
          </a>
        </div>
        <div className="mm-item" onClick={() => buttonHandler("dashboard")}  >
          <a href="">
            <span className="">Short</span>
          </a>
        </div>
        <div className="mm-item" onClick={() => buttonHandler("dashboard")}>
          <a href="">
            <span className="">Shot Sale</span>
          </a>
        </div>
        <div className="mm-item" onClick={() => buttonHandler("dashboard")}>
          <a href="">
            <span className="">Stock Split</span>
          </a>
        </div>
        <div className="mm-item" onClick={() => buttonHandler("dashboard")}>
          <a href="">
            <span className="">Tickers</span>
          </a>
        </div>
        <div className="mm-item" onClick={() => buttonHandler("dashboard")}>
          <a href="">
            <span className="">Transactions</span>
          </a>
        </div>
      </div>
      <ExecComponent></ExecComponent>
    </div>
  );
};

export default Dashboard;
