import React, { useState } from "react";
import ChatPage from "./components/ChatPage";

import "./App.css";

export default function App() {
  const [query, setQuery] = useState("");

  return (
    <div className="app-container">
      {/* Sidebar */}
      <div className="sidebar">
        <div className="sidebar-header">
          <div className="logo-container">
            <div className="logo-placeholder">
            <div className="animated-plane">✈️</div>  
            </div>
          </div>
        </div>
        
        <div className="sidebar-content">
          <div className="sidebar-section">
            <h3>Quick Actions</h3>
            <button className="sidebar-btn">Flight Delays</button>
            <button className="sidebar-btn">Fuel Reports</button>
            <button className="sidebar-btn">Aircraft Info</button>
            <button className="sidebar-btn">Passenger Data</button>
          </div>
          
          <div className="sidebar-section">
            {/* <h3>Recent Queries</h3>
            <div className="recent-query">"Why was 6E215 delayed?"</div>
            <div className="recent-query">"Fuel consumption for AI202"</div>
            <div className="recent-query">"Passenger count for UK945"</div> */}
          </div>

          <div className="indigo-gif-placeholder">
            <div className="gif-container">
              {/* <div className="animated-plane">✈️</div> */}
              <p>Flight Operations</p>
              <p>Live Monitoring</p>
            </div>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="main-content">
        <div className="header">
          <div className="flex items-center justify-center mt-6 space-x-3">
            <img src="/src/assets/indigo-logo.png" alt="logo" className="h-10" />
            <h1 className="text-2xl font-bold text-indigo-700">
              FlightOps Smart Agent 
            </h1>
          </div>
        </div>
        <ChatPage />
      </div>
    </div>
  );
}
