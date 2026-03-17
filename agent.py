import os
from anthropic import Anthropic
from dotenv import load_dotenv
import snowflake.connector

# -----------------------------
# LOAD ENV
# -----------------------------
load_dotenv()

# ✅ Use ENV for local script
client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# -----------------------------
# SNOWFLAKE SEARCH
# -----------------------------
def search_documents(query):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    cursor = conn.cursor()

    # ✅ SAFE query
    sql = """
    SELECT year, file_name, content
    FROM medical_db.records.medical_records
    WHERE content ILIKE %s
    LIMIT 10
    """

    cursor.execute(sql, (f"%{query}%",))
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

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",  # ✅ valid model
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

    except Exception as e:
        print(f"❌ Claude error: {e}")


# -----------------------------
# RUN LOOP
# -----------------------------
if __name__ == "__main__":
    while True:
        user_input = input("\nAsk your question (or type 'exit'): ")

        if user_input.lower() == "exit":
            break

        ask_agent(user_input)
