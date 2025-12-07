import React, { useState, useRef, useEffect } from "react";
import MessageBubble from "./MessageBubble";

const AG_AGENT_ENDPOINT = import.meta.env.VITE_AGENT_ENDPOINT || "http://localhost:8001/agent";

export default function ChatPage() {
    const [messages, setMessages] = useState([
        {
            role: "assistant",
            content: "Hello! 👋 I'm your FlightOps Agent. Ask me anything about flight operations — delays, fuel, passengers, aircraft details, etc.",
            timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
        },
    ]);
    const [input, setInput] = useState("");
    const [loading, setLoading] = useState(false);
    const [agentState, setAgentState] = useState({
        phase: 'idle',
        progress: 0,
        message: ''
    });
    const [tokenUsage, setTokenUsage] = useState({
        planning: null,
        summarization: null,
        total: null
    });
    const messagesEndRef = useRef(null);

    const scrollToBottom = () => {
        messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
    };

    useEffect(() => {
        scrollToBottom();
    }, [messages]);

    async function sendMessage() {
        if (!input.trim()) return;

        const userMessage = { 
            role: "user", 
            content: input,
            timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
        };
        setMessages((prev) => [...prev, userMessage]);
        setLoading(true);
        setInput("");
        
        setAgentState({
            phase: 'thinking',
            progress: 0,
            message: 'Starting analysis...'
        });
        setTokenUsage({
            planning: null,
            summarization: null,
            total: null
        });

        const body = {
            thread_id: "thread-" + Date.now(),
            run_id: "run-" + Date.now(),
            messages: [userMessage],
        };

        try {
            const resp = await fetch(AG_AGENT_ENDPOINT, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(body),
            });

            if (!resp.ok) {
                const errTxt = await resp.text();
                throw new Error(errTxt);
            }

            const reader = resp.body.getReader();
            const decoder = new TextDecoder();
            let buf = "";
            let currentMessageId = null;

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                buf += decoder.decode(value, { stream: true });

                let idx;
                while ((idx = buf.indexOf("\n\n")) !== -1) {
                    const chunk = buf.slice(0, idx).trim();
                    buf = buf.slice(idx + 2);
                    const lines = chunk.split("\n");

                    for (const line of lines) {
                        if (line.startsWith("data: ")) {
                            const payload = line.slice(6).trim();
                            if (!payload) continue;

                            try {
                                const event = JSON.parse(payload);
                                console.log("SSE Event:", event.type, event); // Debug logging
                                handleEvent(event, currentMessageId);
                            } catch (err) {
                                console.warn("Bad SSE line:", payload, err);
                            }
                        }
                    }
                }
            }
        } catch (err) {
            console.error("Error in sendMessage:", err);
            setMessages((prev) => [
                ...prev,
                { 
                    role: "assistant", 
                    content: "⚠️ Error: " + err.message,
                    timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
                },
            ]);
            setAgentState({ phase: 'idle', progress: 0, message: '' });
        } finally {
            setLoading(false);
        }
    }

    function handleEvent(event, currentMessageId) {
        const timestamp = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        
        console.log("Processing event:", event.type); // Debug logging
        
        switch (event.type) {
            case "STATE_UPDATE":
                setAgentState(event.state);
                break;
                
            case "TEXT_MESSAGE_CONTENT":
                if (event.message) {
                    if (event.message.delta) {
                        setMessages(prev => {
                            const existingIdx = prev.findIndex(m => m.id === event.message.id);
                            if (existingIdx >= 0) {
                                const updated = [...prev];
                                updated[existingIdx].content += event.message.delta;
                                updated[existingIdx].timestamp = timestamp;
                                return updated;
                            } else {
                                return [...prev, {
                                    id: event.message.id,
                                    role: event.message.role,
                                    content: event.message.delta,
                                    timestamp: timestamp
                                }];
                            }
                        });
                    } else if (event.message.content) {
                        setMessages(prev => [...prev, {
                            id: event.message.id,
                            role: event.message.role,
                            content: event.message.content,
                            timestamp: timestamp
                        }]);
                    }
                } else if (event.content) {
                    setMessages(prev => [...prev, {
                        role: "assistant",
                        content: event.content,
                        timestamp: timestamp
                    }]);
                }
                break;
                
            case "TOOL_CALL_START":
                setMessages(prev => [...prev, {
                    role: "system",
                    content: `🛠️ Starting ${event.toolCallName}...`,
                    timestamp: timestamp
                }]);
                break;
                
            case "TOKEN_USAGE":
                console.log("TOKEN_USAGE event received:", event.phase, event.usage);
                setTokenUsage(prev => ({
                    ...prev,
                    [event.phase]: event.usage
                }));
                
                // Also show token usage as a system message for visibility
                if (event.phase === 'total' && event.usage) {
                    setMessages(prev => [...prev, {
                        role: "system",
                        content: `📊 Token Usage: ${event.usage.total_tokens || 0} total (${event.usage.prompt_tokens || 0} prompt + ${event.usage.completion_tokens || 0} completion)`,
                        timestamp: timestamp
                    }]);
                }
                break;
                
            case "RUN_FINISHED":
                setAgentState({ phase: 'idle', progress: 0, message: '' });
                break;
                
            case "RUN_ERROR":
                setMessages(prev => [...prev, {
                    role: "assistant",
                    content: "❌ " + event.error,
                    timestamp: timestamp
                }]);
                setAgentState({ phase: 'idle', progress: 0, message: '' });
                break;
                
            default:
                console.log('Unhandled event type:', event.type);
                break;
        }
    }

    const StatusIndicator = () => {
        if (agentState.phase === 'idle' && !tokenUsage.total) return null;
        
        const phaseIcons = {
            thinking: '🧠',
            processing: '🛠️', 
            typing: '✍️',
            finished: '✅',
            idle: '✅'
        };
        
        return (
            <div className={`status-indicator ${agentState.phase}`}>
                <div className="status-content">
                    <span className="status-icon">{phaseIcons[agentState.phase]}</span>
                    <span className="status-message">{agentState.message}</span>
                    
                    {/* Token Usage Display */}
                    {tokenUsage.total && (
                        <div className="token-usage">
                            <div className="token-breakdown">
                                <span className="token-label">Total Tokens: </span>
                                <span className="token-value">{tokenUsage.total.total_tokens || 0}</span>
                            </div>
                            <div className="token-details">
                                <span>Prompt: {tokenUsage.total.prompt_tokens || 0}</span>
                                <span>Completion: {tokenUsage.total.completion_tokens || 0}</span>
                            </div>
                        </div>
                    )}
                    
                    {agentState.progress > 0 && (
                        <div className="progress-bar">
                            <div 
                                className="progress-fill"
                                style={{ width: `${agentState.progress}%` }}
                            ></div>
                        </div>
                    )}
                </div>
            </div>
        );
    };

    return (
        <div className="chat-container">
            <div className="messages-container">
                {messages.map((msg, idx) => (
                    <MessageBubble 
                        key={msg.id || idx} 
                        role={msg.role} 
                        content={msg.content} 
                        timestamp={msg.timestamp}
                    />
                ))}
                
                <StatusIndicator />
                
                {loading && agentState.phase === 'idle' && (
                    <div className="loading-message">
                        <div className="loading-dots">
                            <span></span>
                            <span></span>
                            <span></span>
                        </div>
                        <div className="loading-timestamp">
                            {new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                        </div>
                    </div>
                )}
                <div ref={messagesEndRef} />
            </div>

            <div className="input-container">
                <input
                    type="text"
                    className="message-input"
                    placeholder="Ask about a flight, e.g. 'Why was 6E215 delayed on June 23, 2024?'"
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    onKeyDown={(e) => e.key === "Enter" && sendMessage()}
                    disabled={loading}
                />
                <button
                    onClick={sendMessage}
                    className="send-button"
                    disabled={loading}
                >
                    Send
                </button>
            </div>
        </div>
    );
}
