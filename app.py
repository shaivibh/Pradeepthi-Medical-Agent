import streamlit as st
from anthropic import Anthropic
import snowflake.connector
import os

# --- CONFIG ---
st.set_page_config(page_title="Medical AI Agent", layout="wide")
st.title("🧠 Pradeepthi Medical AI Agent")

# --- API KEY ---
client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# --- SNOWFLAKE SEARCH FUNCTION ---
def search_documents(query):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),  # 👈 use env
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    cursor = conn.cursor()

    sql = f"""
    SELECT year, file_name, content
    FROM medical_records
    WHERE content ILIKE '%{query}%'
    LIMIT 10
    """

    cursor.execute(sql)
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    return results


# --- MAIN APP ---
st.subheader("Ask questions about medical records")

question = st.text_input("Enter your question")

if question:
    with st.spinner("Analyzing medical records..."):

        results = search_documents(question)

        if not results:
            st.error("No relevant data found.")
        else:
            # 🔥 Build context
            context = "\n\n".join([
                f"Year: {row[0]} | File: {row[1]}\n{row[2][:1500]}"
                for row in results
            ])

            # 🤖 Claude prompt
            prompt = f"""
You are a medical assistant.

Answer ONLY using the context below.

Context:
{context}

Question:
{question}

If unsure, say you don't know.
"""

            response = client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=800,
                messages=[{"role": "user", "content": prompt}]
            )

            st.subheader("Answer")
            st.write(response.content[0].text)

            # --- DEBUG PANEL ---
            with st.expander("🔍 Debug Info"):
                st.write("Chunks used:")
                for row in results:
                    st.write(f"- {row[0]} | {row[1]}")
