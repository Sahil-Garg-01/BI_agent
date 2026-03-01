# Skylark Drones – Monday.com Business Intelligence Agent  
## Technical Decision Log

## 1. Objective

The goal of this assignment was to build a conversational AI agent capable of answering founder-level business intelligence queries using live data from monday.com boards containing Deals and Work Orders data.

The system needed to:

- Integrate with monday.com via API (live, no caching)
- Handle messy and inconsistent business data
- Interpret high-level executive questions
- Support follow-up conversational context
- Provide structured business insights
- Display visible tool/API call traces

The final system was designed to prioritize robustness, clarity, and extensibility within the 6-hour timeline.

---

## 2. Architecture Overview

The system follows a clean 3-layer architecture:

### 2.1 Interface Layer
- Streamlit-based conversational UI
- Maintains session-level conversational context
- Displays tool execution traces

### 2.2 Agent Layer
- Gemini 2.5 Flash Lite using native function calling
- Single dynamic query tool (`query_data`)
- Context-aware filter management
- Structured executive response formatting

### 2.3 Data Layer
- Live monday.com GraphQL API integration
- Full normalization layer
- Dynamic filtering, grouping, and aggregation engine
- No preloading or caching

Every user query triggers live API calls to ensure real-time accuracy.

---

## 3. Key Design Decisions

### 3.1 Single Dynamic Query Tool

Instead of implementing fixed tools like `analyze_pipeline` or `analyze_revenue`, the final design uses one flexible tool:

`query_data(board, filters, group_by, metrics)`

This enables:

- Arbitrary filtering
- Dynamic grouping
- Aggregations (count, sum)
- Use across both boards
- Adaptability to unpredictable founder questions

This approach prevents hardcoding analytics logic and improves scalability.

---

### 3.2 Conversational Filter Context

Context is maintained as a dynamic filter object:

{
  "filters": {
    "sector": "...",
    "stage": "...",
    "quarter": "...",
    ...
  }
}

When a user modifies a single constraint (e.g., “last quarter”), previous filters are preserved unless explicitly changed.

This allows natural follow-ups such as:
- “What about last quarter?”
- “Only proposal stage.”
- “Now show renewables.”

---

### 3.3 Live monday.com Integration

All queries trigger fresh GraphQL API calls to monday.com.

No:
- Local caching
- Preloaded datasets
- Offline processing

This ensures accuracy and satisfies the assignment’s requirement for live integration.

---

### 3.4 Robust Data Normalization

The data is intentionally messy. The normalization layer was designed to handle:

- Invalid numeric values → converted to None (never coerced to 0)
- True zero preserved as 0.0
- Safe date parsing
- Text normalization (case consistency)
- Derived metric computation:
  - Weighted pipeline value
  - Billing ratio
  - Collection ratio

Data quality flags are attached to records to detect:
- Missing values
- Missing stage
- Negative billing amounts
- High receivables

This ensures financial correctness and analytical integrity.

---

### 3.5 Executive-Oriented Response Structure

The system prompt enforces structured responses:

1. Brief Summary  
2. Key Metrics  
3. Observations  
4. Risks / Executive Insight  

This ensures the agent provides decision-ready output rather than raw data dumps.

---

## 4. Trade-offs Considered

### 4.1 Why Not Use LangChain or LangGraph?

LangChain and LangGraph were intentionally not used.

While these frameworks provide advanced orchestration (multi-agent graphs, complex memory flows, tool routing), the assignment scope did not require multi-step agent graphs or complex state machines.

Using native Gemini function-calling provided:

- Lower architectural complexity
- Reduced abstraction layers
- Easier debugging
- Clearer tool execution traces
- Faster development within the 6-hour constraint

For a single dynamic BI query engine, introducing LangChain or LangGraph would have added unnecessary overhead without improving functionality.

The current architecture remains modular and can be migrated to LangGraph if future requirements demand advanced orchestration.

---

### 4.2 Why Not Build a SQL or Pandas Query Engine?

A SQL abstraction layer or code-execution agent was intentionally avoided to:

- Keep the system lightweight
- Avoid unnecessary complexity
- Reduce security risks from dynamic code execution
- Maintain predictable aggregation logic

Instead, a controlled filtering and aggregation engine was implemented in Python, sufficient for BI-style queries.

---

### 4.3 Why One Tool Instead of Multiple Fixed Tools?

Initial iterations used task-specific tools (pipeline, revenue).

This approach was replaced because:

- Founder queries are unpredictable
- Hardcoded analytics restrict flexibility
- A generic query interface scales better

The final dynamic tool design supports arbitrary filtering and grouping across both boards.

---

## 5. Data Resilience Strategy

To handle messy real-world business data:

- Invalid numeric inputs are treated as None (not zero)
- Missing dates are preserved as None
- Derived ratios include zero-division protection
- No silent coercion of invalid values
- Data-quality caveats are surfaced in responses when relevant

This prevents misleading financial analysis.

## 7. Conclusion

The final solution:

- Integrates live with monday.com
- Handles messy business data robustly
- Supports dynamic BI-style queries
- Maintains conversational context
- Produces executive-level insights
- Displays visible tool execution traces
- Avoids caching and overengineering

The system balances flexibility, reliability, and simplicity within the given time constraints and satisfies all core assignment requirements.