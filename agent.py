import os
from anthropic import Anthropic
from dotenv import load_dotenv
import snowflake.connector

# -----------------------------
# LOAD ENV
# -----------------------------
load_dotenv()

client = Anthropic(api_key=st.secrets["ANTHROPIC_API_KEY"])

# -----------------------------
# SNOWFLAKE SEARCH
# -----------------------------
def search_documents(query):
    conn = snowflake.connector.connect(
        user=st.secrets("SNOWFLAKE_USER"),
        password=st.secrets("SNOWFLAKE_PASSWORD"),
        account=st.secrets("SNOWFLAKE_ACCOUNT"),
        warehouse=st.secrets("SNOWFLAKE_WAREHOUSE"),
        database=st.secrets("SNOWFLAKE_DATABASE"),
        schema=st.secrets("SNOWFLAKE_SCHEMA")
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


# -----------------------------
# MAIN AGENT FUNCTION
# -----------------------------
def ask_agent(question):
    print("\n🔍 Searching medical records...\n")

    results = search_documents(question)

    if not results:
        print("⚠️ No relevant data found.")
        return

    # 🔥 Build context
    context = "\n\n".join([
        f"Year: {row[0]} | File: {row[1]}\n{row[2][:1500]}"
        for row in results
    ])

    print("🧠 Sending to Claude...\n")

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        messages=[
            {
                "role": "user",
                "content": f"""
You are a medical assistant analyzing long-term patient records.

Instructions:
- Use ONLY the provided context
- Identify patterns across years
- Highlight key medications
- Explain risks clearly
- Be structured and concise

Context:
{context}

Question:
{question}

If unsure, say you don't know.
"""
            }
        ]
    )

    print("\n--- ✅ ANSWER ---\n")
    print(response.content[0].text)


# -----------------------------
# RUN LOOP
# -----------------------------
if __name__ == "__main__":
    while True:
        user_input = input("\nAsk your question (or type 'exit'): ")

        if user_input.lower() == "exit":
            break

        ask_agent(user_input)