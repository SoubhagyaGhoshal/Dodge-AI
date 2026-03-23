/**
 * SAP O2C Graph Intelligence - Dodge AI Style
 */

let network = null;
let allNodes = new vis.DataSet();
let allEdges = new vis.DataSet();
let chatHistory = [];
let isLoading = false;
let overlayActive = false;

// ── Graph Initialization ───────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  initGraph();
});

async function initGraph() {
  try {
    const res = await fetch('/api/graph/overview');
    const data = await res.json();
    renderGraph(data.nodes, data.edges);
  } catch (e) {
    console.error("Failed to load graph", e);
  }
}

function getBaseColor(type) {
  // Dodge AI mockup style: Light airy blue nodes mostly, some red
  const isCore = ['SalesOrder', 'BillingDocument'].includes(type);
  return {
    background: isCore ? '#8cb4f5' : '#ffffff',
    border: isCore ? '#5c8dec' : '#8cb4f5',
    highlight: { background: '#2f6ded', border: '#1c55cc' },
    hover: { background: '#5c8dec', border: '#2f6ded' }
  };
}

function renderGraph(nodes, edges) {
  const container = document.getElementById('graph-canvas');
  
  const formattedNodes = nodes.map(n => ({
    id: n.id,
    color: getBaseColor(n.type),
    size: ['SalesOrder', 'BillingDocument'].includes(n.type) ? 8 : 4,
    borderWidth: 1.5,
    _data: n,
    // Add small red dots for specific properties to mimic mockup
    shape: ['Product', 'Plant'].includes(n.type) ? 'dot' : 'dot',
  }));
  
  const formattedEdges = edges.map((e, i) => ({
    id: `e${i}`,
    from: e.from,
    to: e.to,
    color: { color: 'rgba(140, 180, 245, 0.4)', highlight: 'rgba(47, 109, 237, 0.8)' },
    width: 1,
    arrows: { to: { enabled: false } }, // Mockup shows undirected-looking lines
  }));
  
  allNodes = new vis.DataSet(formattedNodes);
  allEdges = new vis.DataSet(formattedEdges);
  
  network = new vis.Network(container, { nodes: allNodes, edges: allEdges }, {
    physics: {
      barnesHut: { gravitationalConstant: -1000, centralGravity: 0.1, springLength: 100 },
      stabilization: { iterations: 150 }
    },
    interaction: {
      hover: true,
      zoomSpeed: 0.6,
      tooltipDelay: 100
    }
  });

  // Events
  network.on('click', function(params) {
    if (params.nodes.length > 0) {
      showNodePopup(params.nodes[0], params.pointer.DOM);
    } else {
      hideNodePopup();
    }
  });

  network.on('dragStart', () => hideNodePopup());
  network.on('zoom', () => hideNodePopup());
}

// ── Node Popup ─────────────────────────────────────────────────────────────
function showNodePopup(nodeId, domPos) {
  const nodeData = allNodes.get(nodeId)?._data;
  if (!nodeData) return;

  const popup = document.getElementById('node-info');
  popup.innerHTML = `
    <div style="font-weight:600;margin-bottom:6px;font-size:11px;">${nodeData.type} ${nodeData.key || ''}</div>
  `;
  
  const skip = new Set(['type', 'key', 'id', 'label', 'title', 'group', 'color', 'shape']);
  Object.entries(nodeData).forEach(([k, v]) => {
    if (skip.has(k) || !v) return;
    popup.innerHTML += `<div class="prop-row"><div class="prop-key">${k}:</div><div class="prop-val">${v}</div></div>`;
  });
  
  // Position the popup exactly where clicked (like the mockup native tooltip)
  popup.style.display = 'block';
  
  // Keep it within bounds
  const rect = document.getElementById('graph-panel').getBoundingClientRect();
  let left = domPos.x + 15;
  let top = domPos.y + 15;
  
  if (left + 240 > rect.width) left = domPos.x - 250;
  if (top + popup.offsetHeight > rect.height) top = domPos.y - popup.offsetHeight - 10;
  
  popup.style.left = left + 'px';
  popup.style.top = top + 'px';
}

function hideNodePopup() {
  document.getElementById('node-info').style.display = 'none';
}

function toggleOverlay() {
  overlayActive = !overlayActive;
  const btn = document.querySelector('.graph-btn.dark');
  if (overlayActive) {
    btn.innerHTML = btn.innerHTML.replace('Hide', 'Show');
    allNodes.forEach(n => {
      if (!['SalesOrder', 'BillingDocument'].includes(n._data.type)) {
        allNodes.update({ id: n.id, hidden: true });
      }
    });
  } else {
    btn.innerHTML = btn.innerHTML.replace('Show', 'Hide');
    allNodes.forEach(n => allNodes.update({ id: n.id, hidden: false }));
  }
}

// ── Chat Logic ─────────────────────────────────────────────────────────────
function handleKey(e) {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    sendMessage();
  }
}

async function sendMessage() {
  const input = document.getElementById('chat-input');
  const message = input.value.trim();
  if (!message || isLoading) return;
  
  input.value = '';
  appendUserMessage(message);
  chatHistory.push({ role: 'user', content: message });
  
  const typingId = appendTyping();
  isLoading = true;
  document.getElementById('send-btn').disabled = true;
  
  try {
    const res = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message, history: chatHistory.slice(-6) }),
    });
    
    document.getElementById(typingId)?.remove();
    
    if (!res.ok) {
      const err = await res.json().catch(()=>({detail: 'Error'}));
      appendAIMessage(err.detail || 'Error getting response.');
      return;
    }
    
    const data = await res.json();
    appendAIMessage(data.response, data.sql_used);
    chatHistory.push({ role: 'assistant', content: data.response });
    
    // Highlight referenced nodes very subtly
    if (data.referenced_nodes && data.referenced_nodes.length > 0) {
      data.referenced_nodes.forEach(nid => {
        if (allNodes.get(nid)) {
          allNodes.update({ id: nid, size: 12, borderWidth: 3 });
          setTimeout(() => {
            const size = ['SalesOrder', 'BillingDocument'].includes(nid.split(':')[0]) ? 8 : 4;
            allNodes.update({ id: nid, size: size, borderWidth: 1.5 });
          }, 3000);
        }
      });
      network.focus(data.referenced_nodes[0], { scale: 1.2, animation: { duration: 800 } });
    }
  } catch(e) {
    document.getElementById(typingId)?.remove();
    appendAIMessage("Connection error.");
  } finally {
    isLoading = false;
    document.getElementById('send-btn').disabled = false;
  }
}

function appendUserMessage(text) {
  const msgs = document.getElementById('messages');
  msgs.innerHTML += `<div class="msg-user"><div class="bubble">${escapeHtml(text)}</div></div>`;
  msgs.scrollTop = msgs.scrollHeight;
}

function appendAIMessage(text, sql) {
  const msgs = document.getElementById('messages');
  let format = text.split('\n').map(L => {
    L = L.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
    return L ? `<p>${L}</p>` : '';
  }).join('');
  
  let sqlBadge = sql ? `<div class="sql-badge" title="${escapeHtml(sql)}">SQL Executed</div>` : '';
  
  msgs.innerHTML += `
    <div class="msg-ai">
      <div class="ai-header">
        <div class="ai-avatar">D</div>
        <div class="ai-identity">
          <div class="ai-name">Dodge AI</div>
          <div class="ai-role">Graph Agent</div>
        </div>
      </div>
      <div class="bubble">
        ${format}
        ${sqlBadge}
      </div>
    </div>
  `;
  msgs.scrollTop = msgs.scrollHeight;
}

function appendTyping() {
  const id = 'typing-' + Date.now();
  const msgs = document.getElementById('messages');
  msgs.innerHTML += `
    <div class="msg-ai" id="${id}">
      <div class="ai-header">
        <div class="ai-avatar">D</div>
        <div class="ai-identity"><div class="ai-name">Dodge AI</div><div class="ai-role">Graph Agent</div></div>
      </div>
      <div class="bubble"><span class="typing-dot"></span><span class="typing-dot" style="animation-delay:0.2s"></span><span class="typing-dot" style="animation-delay:0.4s"></span></div>
    </div>
  `;
  msgs.scrollTop = msgs.scrollHeight;
  return id;
}

function escapeHtml(str) { return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); }
