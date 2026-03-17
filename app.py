import streamlit as st
from anthropic import Anthropic
import snowflake.connector
import os

# --- CONFIG ---
st.set_page_config(page_title="Medical AI Agent", layout="wide")
st.title("🧠 Pradeepthi Medical AI Agent")

# --- API KEY ---
client = Anthropic(api_key=st.secrets["ANTHROPIC_API_KEY"])

# --- SNOWFLAKE SEARCH FUNCTION ---
def search_documents(query):
    try:
        conn = snowflake.connector.connect(
            user=st.secrets["SNOWFLAKE_USER"],
            password=st.secrets["SNOWFLAKE_PASSWORD"],
            account=st.secrets["SNOWFLAKE_ACCOUNT"],
            warehouse=st.secrets["SNOWFLAKE_WAREHOUSE"],
            database=st.secrets["SNOWFLAKE_DATABASE"],
            schema=st.secrets["SNOWFLAKE_SCHEMA"]
        )

        cursor = conn.cursor()

        # ✅ SAFE QUERY (prevents SQL injection)
        sql = """
        SELECT year, file_name, content
        FROM medical_records
        WHERE content ILIKE %s
        LIMIT 10
        """

        cursor.execute(sql, (f"%{query}%",))
        results = cursor.fetchall()

        cursor.close()
        conn.close()

        return results

    except Exception as e:
        st.error(f"Snowflake error: {e}")
        return []


# --- MAIN APP ---
st.subheader("Ask questions about medical records")

question = st.text_input("Enter your question")

if question:
    with st.spinner("Analyzing medical records..."):

        results = search_documents(question)

        if not results:
            st.error("No relevant data found.")
        else:
            # Build context
            context = "\n\n".join([
                f"Year: {row[0]} | File: {row[1]}\n{row[2][:1500]}"
                for row in results
            ])

            prompt = f"""
You are a medical assistant.

STRICT RULES:
- Answer ONLY using the provided context
- Do NOT assume anything
- If information is missing, say "I don't know"

Context:
{context}

Question:
{question}
"""

            try:
                response = client.messages.create(
                    model="claude-3-haiku-20240307",
                    max_tokens=800,
                    temperature=0,
                    messages=[
                        {"role": "user", "content": prompt}
                    ]
                )

                # ✅ safer extraction
                answer = ""
                if response.content:
                    for block in response.content:
                        if hasattr(block, "text"):
                            answer += block.text

                st.subheader("Answer")
                st.write(answer)

            except Exception as e:
                st.error(f"Claude API error: {e}")

            # --- DEBUG ---
            with st.expander("🔍 Debug Info"):
                st.write("Chunks used:")
                for row in results:
                    st.write(f"- {row[0]} | {row[1]}")