import React from "react";

export default function MessageBubble({ role, content, timestamp }) {
  return (
    <div className={`message ${role}-message`}>
      <div className="message-content">
        <div className="message-text">{content}</div>
        <div className="message-timestamp">{timestamp}</div>
      </div>
    </div>
  );
}
