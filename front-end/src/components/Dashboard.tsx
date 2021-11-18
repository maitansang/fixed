import React, { useEffect, useState } from "react";
import { getCurrentUser } from "../services/auth.service";
import ExecComponent from "./ExecComponent";

const Dashboard: React.FC = () => {
  const currentUser = getCurrentUser();
  const [runScript, setRunScript] = useState("");
  const menus = [
    { id: 0, name: ["Dashboard","dashboard"] },
    { id: 1, name: ["Aggregates","aggregates"] },
    { id: 2, name: ["Average Volume","average_volume"] },
    { id: 3, name: ["Trades","trades"] },
    { id: 4, name: ["Breakouthist","breakouthist"]},
    { id: 5, name: ["Changepct","changepct"] },
    { id: 6, name: ["Changepctall","changepctall"] },
    { id: 7, name: ["Lob","lob"] },
    { id: 8, name: ["Lov","lov"] },
    { id: 9, name: ["Pattern Features","pattern_features"] },
    { id: 10, name: ["Short","short"] },
    { id: 11, name: ["Shot Sale","short_sale"] },
    { id: 12, name: ["Stock Split","stock_split"] },
    { id: 13, name: ["Tickers","tickers"] },
    { id: 14, name: ["Transactions","transactions"] },
  ];

  const buttonHandler = (text: any) => {
    console.log("=====", text);
    setRunScript(text);
  };

  return (
    <div className="container">
      <div className="right-menu-container">
        {menus.map((item, index) => {
          return (
            <div
              className="mm-item"
              onClick={() => buttonHandler(item.name[1])}
              key={index}
            >
              <a>
                <span className="">{item.name[0]}</span>
              </a>
            </div>
          );
        })}
      </div>
      <ExecComponent key="" value={runScript}></ExecComponent>
    </div>
  );
};;;

export default Dashboard;
