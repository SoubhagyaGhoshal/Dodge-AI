"""
Graph builder: loads data from o2c.db and constructs a NetworkX directed graph
of business entities and their relationships.
"""
import sqlite3
import os
import networkx as nx
from typing import Dict, List, Any, Optional

DB_PATH = os.path.join(os.path.dirname(__file__), "o2c.db")

# Node type colors for the frontend
NODE_COLORS = {
    "SalesOrder": "#4A90D9",
    "SalesOrderItem": "#A8D4F5",
    "BillingDocument": "#27AE60",
    "OutboundDelivery": "#F39C12",
    "BusinessPartner": "#8E44AD",
    "Product": "#E74C3C",
    "Plant": "#795548",
    "JournalEntry": "#78909C",
    "Payment": "#00BCD4",
}

NODE_SHAPES = {
    "SalesOrder": "box",
    "SalesOrderItem": "ellipse",
    "BillingDocument": "diamond",
    "OutboundDelivery": "triangle",
    "BusinessPartner": "circle",
    "Product": "star",
    "Plant": "square",
    "JournalEntry": "hexagon",
    "Payment": "dot",
}


def make_node_id(node_type: str, key: str) -> str:
    return f"{node_type}:{key}"


class O2CGraph:
    def __init__(self):
        self.G = nx.DiGraph()
        self.db_path = DB_PATH
        self._loaded = False

    def _conn(self):
        return sqlite3.connect(self.db_path)

    def load(self):
        """Build the full graph from the database."""
        conn = self._conn()
        conn.row_factory = sqlite3.Row
        
        self._load_sales_orders(conn)
        self._load_billing_documents(conn)
        self._load_outbound_deliveries(conn)
        self._load_business_partners(conn)
        self._load_products(conn)
        self._load_plants(conn)
        self._load_journal_entries(conn)
        self._load_payments(conn)
        self._load_edges(conn)
        
        conn.close()
        self._loaded = True
        print(f"✅ Graph loaded: {self.G.number_of_nodes()} nodes, {self.G.number_of_edges()} edges")

    def _add_node(self, node_type: str, key: str, **attrs):
        if not key:
            return
        key = str(key)
        nid = make_node_id(node_type, key)
        # Clean attrs: convert None to empty string, booleans to int
        clean_attrs = {}
        for k, v in attrs.items():
            if v is None:
                clean_attrs[k] = ""
            elif isinstance(v, bool):
                clean_attrs[k] = int(v)
            else:
                clean_attrs[k] = v
        self.G.add_node(nid,
                        type=node_type,
                        key=key,
                        color=NODE_COLORS.get(node_type, "#999"),
                        shape=NODE_SHAPES.get(node_type, "dot"),
                        label=f"{node_type}\n{key[:20]}",
                        **clean_attrs)

    def _add_edge(self, src_type: str, src_key: str, dst_type: str, dst_key: str, relationship: str):
        src = make_node_id(src_type, src_key)
        dst = make_node_id(dst_type, dst_key)
        if self.G.has_node(src) and self.G.has_node(dst):
            self.G.add_edge(src, dst, relationship=relationship, label=relationship)

    def _load_sales_orders(self, conn):
        for row in conn.execute("SELECT * FROM sales_order_headers"):
            r = dict(row)
            self._add_node("SalesOrder", r["salesOrder"],
                           totalNetAmount=r.get("totalNetAmount"),
                           currency=r.get("transactionCurrency"),
                           creationDate=r.get("creationDate"),
                           soldToParty=r.get("soldToParty"),
                           deliveryStatus=r.get("overallDeliveryStatus"),
                           billingStatus=r.get("overallOrdReltdBillgStatus"),
                           salesOrg=r.get("salesOrganization"),
                           requestedDeliveryDate=r.get("requestedDeliveryDate"))

        for row in conn.execute("SELECT * FROM sales_order_items"):
            r = dict(row)
            key = f"{r['salesOrder']}-{r['salesOrderItem']}"
            self._add_node("SalesOrderItem", key,
                           salesOrder=r.get("salesOrder"),
                           item=r.get("salesOrderItem"),
                           material=r.get("material"),
                           quantity=r.get("requestedQuantity"),
                           unit=r.get("requestedQuantityUnit"),
                           netAmount=r.get("netAmount"),
                           plant=r.get("productionPlant"))

    def _load_billing_documents(self, conn):
        for row in conn.execute("SELECT * FROM billing_document_headers"):
            r = dict(row)
            self._add_node("BillingDocument", r["billingDocument"],
                           docType=r.get("billingDocumentType"),
                           totalNetAmount=r.get("totalNetAmount"),
                           currency=r.get("transactionCurrency"),
                           isCancelled=bool(r.get("billingDocumentIsCancelled")),
                           cancelledDoc=r.get("cancelledBillingDocument"),
                           accountingDocument=r.get("accountingDocument"),
                           soldToParty=r.get("soldToParty"),
                           billingDate=r.get("billingDocumentDate"))

    def _load_outbound_deliveries(self, conn):
        for row in conn.execute("SELECT * FROM outbound_delivery_headers"):
            r = dict(row)
            self._add_node("OutboundDelivery", r["outboundDelivery"],
                           shippingPoint=r.get("shippingPoint"),
                           deliveryDate=r.get("deliveryDate"),
                           goodsMovementDate=r.get("actualGoodsMovementDate"),
                           status=r.get("overallDeliveryStatus"))

    def _load_business_partners(self, conn):
        for row in conn.execute("SELECT * FROM business_partners"):
            r = dict(row)
            self._add_node("BusinessPartner", r["businessPartner"],
                           name=r.get("businessPartnerName"),
                           bpType=r.get("businessPartnerType"),
                           country=r.get("country"),
                           city=r.get("city"),
                           region=r.get("region"))
        
        # Also add minimal nodes for any soldToParty references not in business_partners
        known = {d["key"] for n, d in self.G.nodes(data=True) if d.get("type") == "BusinessPartner"}
        for row in conn.execute("SELECT DISTINCT soldToParty FROM sales_order_headers WHERE soldToParty != ''"):
            bp = row[0]
            if bp and bp not in known:
                self._add_node("BusinessPartner", bp)
                known.add(bp)

    def _load_products(self, conn):
        loaded = set()
        for row in conn.execute("""
            SELECT p.product, pd.productDescription, p.productType, p.baseUnit, p.grossWeight, p.netWeight
            FROM products p
            LEFT JOIN product_descriptions pd ON p.product = pd.product AND pd.language = 'EN'
        """):
            r = dict(zip(["product", "description", "productType", "baseUnit", "grossWeight", "netWeight"], row))
            self._add_node("Product", r["product"],
                           description=r.get("description"),
                           productType=r.get("productType"),
                           baseUnit=r.get("baseUnit"),
                           grossWeight=r.get("grossWeight"))
            loaded.add(r["product"])
        
        # Also add from sales_order_items materials
        for row in conn.execute("SELECT DISTINCT material FROM sales_order_items WHERE material != ''"):
            mat = row[0]
            if mat and mat not in loaded:
                self._add_node("Product", mat)
                loaded.add(mat)

    def _load_plants(self, conn):
        loaded = set()
        for row in conn.execute("SELECT * FROM plants"):
            r = dict(row)
            self._add_node("Plant", r["plant"],
                           plantName=r.get("plantName"),
                           companyCode=r.get("companyCode"),
                           country=r.get("country"),
                           city=r.get("city"))
            loaded.add(r["plant"])
        
        # Add from sales_order_items
        for row in conn.execute("SELECT DISTINCT productionPlant FROM sales_order_items WHERE productionPlant != ''"):
            p = row[0]
            if p and p not in loaded:
                self._add_node("Plant", p)
                loaded.add(p)

    def _load_journal_entries(self, conn):
        for row in conn.execute("""
            SELECT DISTINCT accountingDocument, companyCode, fiscalYear, postingDate, 
                            businessPartner, amountInCompanyCodeCurrency, companyCodeCurrency
            FROM journal_entry_items_accounts_receivable
        """):
            r = dict(zip(["accountingDocument","companyCode","fiscalYear","postingDate",
                           "businessPartner","amount","currency"], row))
            self._add_node("JournalEntry", r["accountingDocument"],
                           companyCode=r.get("companyCode"),
                           fiscalYear=r.get("fiscalYear"),
                           postingDate=r.get("postingDate"),
                           businessPartner=r.get("businessPartner"),
                           amount=r.get("amount"),
                           currency=r.get("currency"))

    def _load_payments(self, conn):
        for row in conn.execute("""
            SELECT DISTINCT paymentDocument, businessPartner, 
                            amountInCompanyCodeCurrency, companyCodeCurrency, clearingDate
            FROM payments_accounts_receivable
            WHERE paymentDocument != '' AND paymentDocument IS NOT NULL
        """):
            r = dict(zip(["paymentDocument","businessPartner","amount","currency","clearingDate"], row))
            self._add_node("Payment", r["paymentDocument"],
                           businessPartner=r.get("businessPartner"),
                           amount=r.get("amount"),
                           currency=r.get("currency"),
                           clearingDate=r.get("clearingDate"))

    def _load_edges(self, conn):
        # SalesOrder → SalesOrderItem (HAS_ITEM)
        for row in conn.execute("SELECT DISTINCT salesOrder, salesOrderItem FROM sales_order_items"):
            so, item = row
            item_key = f"{so}-{item}"
            self._add_edge("SalesOrder", so, "SalesOrderItem", item_key, "HAS_ITEM")

        # SalesOrder → BusinessPartner (SOLD_TO)
        for row in conn.execute("SELECT salesOrder, soldToParty FROM sales_order_headers WHERE soldToParty != ''"):
            so, bp = row
            self._add_edge("SalesOrder", so, "BusinessPartner", bp, "SOLD_TO")

        # SalesOrderItem → Product (REFERENCES_PRODUCT)
        for row in conn.execute("SELECT salesOrder, salesOrderItem, material FROM sales_order_items WHERE material != ''"):
            so, item, mat = row
            item_key = f"{so}-{item}"
            self._add_edge("SalesOrderItem", item_key, "Product", mat, "REFERENCES_PRODUCT")

        # SalesOrderItem → Plant (PRODUCED_AT)
        for row in conn.execute("SELECT salesOrder, salesOrderItem, productionPlant FROM sales_order_items WHERE productionPlant != ''"):
            so, item, plant = row
            item_key = f"{so}-{item}"
            self._add_edge("SalesOrderItem", item_key, "Plant", plant, "PRODUCED_AT")

        # BillingDocument → BusinessPartner (BILLED_TO)
        for row in conn.execute("SELECT billingDocument, soldToParty FROM billing_document_headers WHERE soldToParty != ''"):
            bd, bp = row
            self._add_edge("BillingDocument", bd, "BusinessPartner", bp, "BILLED_TO")

        # BillingDocument → JournalEntry (POSTED_TO)
        for row in conn.execute("SELECT billingDocument, accountingDocument FROM billing_document_headers WHERE accountingDocument != ''"):
            bd, je = row
            self._add_edge("BillingDocument", bd, "JournalEntry", je, "POSTED_TO")

        # BillingDocument → BillingDocument (CANCELS)
        for row in conn.execute("""
            SELECT billingDocument, cancelledBillingDocument 
            FROM billing_document_headers 
            WHERE cancelledBillingDocument != '' AND cancelledBillingDocument IS NOT NULL
        """):
            bd, cancelled = row
            self._add_edge("BillingDocument", bd, "BillingDocument", cancelled, "CANCELS")

        # SalesOrder → BillingDocument (via billing_document_items.salesDocument)
        for row in conn.execute("""
            SELECT DISTINCT bdi.salesDocument, bdi.billingDocument 
            FROM billing_document_items bdi
            WHERE bdi.salesDocument != ''
        """):
            so, bd = row
            self._add_edge("SalesOrder", so, "BillingDocument", bd, "BILLED_AS")

        # OutboundDelivery → SalesOrder (DELIVERS_FOR)
        for row in conn.execute("""
            SELECT DISTINCT odi.salesOrder, odi.outboundDelivery
            FROM outbound_delivery_items odi
            WHERE odi.salesOrder != ''
        """):
            so, od = row
            self._add_edge("OutboundDelivery", od, "SalesOrder", so, "DELIVERS_FOR")

        # OutboundDelivery → Plant (SHIPPED_FROM)
        for row in conn.execute("""
            SELECT DISTINCT outboundDelivery, plant 
            FROM outbound_delivery_items 
            WHERE plant != ''
        """):
            od, plant = row
            self._add_edge("OutboundDelivery", od, "Plant", plant, "SHIPPED_FROM")

        # Payment → JournalEntry (via accounting document)
        for row in conn.execute("""
            SELECT DISTINCT p.paymentDocument, j.accountingDocument
            FROM payments_accounts_receivable p
            JOIN journal_entry_items_accounts_receivable j 
                ON p.companyCode = j.companyCode AND p.fiscalYear = j.fiscalYear 
                AND p.accountingDocument = j.accountingDocument
            WHERE p.paymentDocument != ''
        """):
            pay, je = row
            self._add_edge("Payment", pay, "JournalEntry", je, "ASSOCIATED_WITH")

        print(f"  Edges loaded: {self.G.number_of_edges()}")

    def to_vis_data(self, max_nodes: int = 200) -> Dict:
        """Convert graph to vis.js compatible format, sampling if too large."""
        nodes = []
        edges = []
        
        # Prioritize certain high-value node types first
        priority_types = ["SalesOrder", "BillingDocument", "OutboundDelivery", "BusinessPartner"]
        other_types = ["SalesOrderItem", "Product", "Plant", "JournalEntry", "Payment"]
        
        selected_nodes = set()
        for ntype in priority_types + other_types:
            for nid, data in self.G.nodes(data=True):
                if data.get("type") == ntype and len(selected_nodes) < max_nodes:
                    selected_nodes.add(nid)
        
        for nid in selected_nodes:
            data = dict(self.G.nodes[nid])
            nodes.append({
                "id": nid,
                "label": f"{data.get('type', '')}\n{data.get('key', '')[:16]}",
                "title": self._node_title(data),
                "color": data.get("color", "#999"),
                "type": data.get("type"),
                "key": data.get("key"),
                "group": data.get("type"),
                **{k: v for k, v in data.items() if k not in ("color", "shape", "label")}
            })
        
        for src, dst, edata in self.G.edges(data=True):
            if src in selected_nodes and dst in selected_nodes:
                edges.append({
                    "from": src,
                    "to": dst,
                    "label": edata.get("relationship", ""),
                    "arrows": "to"
                })
        
        return {"nodes": nodes, "edges": edges}

    def get_node_data(self, node_id: str) -> Optional[Dict]:
        """Get full node data."""
        if not self.G.has_node(node_id):
            return None
        return dict(self.G.nodes[node_id])

    def get_neighbors(self, node_id: str, depth: int = 1) -> Dict:
        """Get neighboring nodes and edges for a given node."""
        if not self.G.has_node(node_id):
            return {"nodes": [], "edges": []}
        
        nodes = {}
        edges = []
        
        # Add source node
        nodes[node_id] = dict(self.G.nodes[node_id])
        
        # Get successors and predecessors
        for successor in self.G.successors(node_id):
            nodes[successor] = dict(self.G.nodes[successor])
            edata = self.G.edges[node_id, successor]
            edges.append({"from": node_id, "to": successor, "label": edata.get("relationship", ""), "arrows": "to"})
        
        for predecessor in self.G.predecessors(node_id):
            nodes[predecessor] = dict(self.G.nodes[predecessor])
            edata = self.G.edges[predecessor, node_id]
            edges.append({"from": predecessor, "to": node_id, "label": edata.get("relationship", ""), "arrows": "to"})
        
        # Format nodes for vis.js
        formatted_nodes = []
        for nid, data in nodes.items():
            formatted_nodes.append({
                "id": nid,
                "label": f"{data.get('type', '')}\n{data.get('key', '')[:16]}",
                "title": self._node_title(data),
                "color": data.get("color", "#999"),
                "type": data.get("type"),
                "key": data.get("key"),
                **{k: v for k, v in data.items() if k not in ("color", "shape", "label")}
            })
        
        return {"nodes": formatted_nodes, "edges": edges}

    def _node_title(self, data: Dict) -> str:
        """Build an HTML tooltip for a node."""
        parts = [f"<b>{data.get('type', 'Unknown')}</b>: {data.get('key', '')}"]
        skip = {"type", "key", "color", "shape", "label"}
        for k, v in data.items():
            if k not in skip and v is not None and v != "" and v != False:
                parts.append(f"{k}: {v}")
        return "<br>".join(parts)

    def get_stats(self) -> Dict:
        """Return graph statistics."""
        type_counts = {}
        for _, data in self.G.nodes(data=True):
            t = data.get("type", "Unknown")
            type_counts[t] = type_counts.get(t, 0) + 1
        
        rel_counts = {}
        for _, _, data in self.G.edges(data=True):
            r = data.get("relationship", "Unknown")
            rel_counts[r] = rel_counts.get(r, 0) + 1
        
        return {
            "total_nodes": self.G.number_of_nodes(),
            "total_edges": self.G.number_of_edges(),
            "node_types": type_counts,
            "relationship_types": rel_counts,
        }

    def find_nodes_by_type(self, node_type: str, limit: int = 50) -> List[Dict]:
        """List nodes of a given type."""
        result = []
        for nid, data in self.G.nodes(data=True):
            if data.get("type") == node_type:
                result.append({"id": nid, "key": data.get("key"), **data})
                if len(result) >= limit:
                    break
        return result


# Singleton
_graph: Optional[O2CGraph] = None


def get_graph() -> O2CGraph:
    global _graph
    if _graph is None or not _graph._loaded:
        _graph = O2CGraph()
        _graph.load()
    return _graph
