import React, { useEffect, useState } from "react";
import { getCurrentUser } from "../services/auth.service";
import ExecComponent from "./ExecComponent";

const Dashboard: React.FC = () => {
  const currentUser = getCurrentUser();
  const [runScript, setRunScript] = useState("");;
  const [menus, setMenus] = useState([
    { id: 0, name: "Dashboard" },
    { id: 1, name: "Aggreagates" },
    { id: 2, name: "Average Volume" },
    { id: 3, name: "Trades" },
    { id: 4, name: "Breakouthis" },
    { id: 5, name: "Changepct" },
    { id: 6, name: "Changepctall" },
    { id: 7, name: "Lob" },
    { id: 8, name: "Lov" },
    { id: 9, name: "Pattern Features" },
    { id: 10, name: "Short" },
    { id: 11, name: "Shot Sale" },
    { id: 12, name: "Stock Split" },
    { id: 13, name: "Tickers" },
    { id: 14, name: "Transactions" },
  ]);

  const buttonHandler = (text: any) => {
    console.log("=====",  text);;
    setRunScript(text);
  };
  useEffect(() => {
    setRunScript(runScript);
  }, [menus])
  return (
    <div className="container">
      <div className="right-menu-container">
        {menus.map((item, index) => {
          return (
            <div className="mm-item" onClick={() => buttonHandler(item.name)}>
              <a href="">
                <span className="">{item.name}</span>
              </a>
            </div>
          );
        })}

      </div>
      <ExecComponent key="" value={runScript}></ExecComponent>

    </div>
  );
};

export default Dashboard;
