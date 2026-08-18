# Wipes all ledger data: every transaction, posting, outbox event, audit entry,
# user, and account. Account and user ids restart from 1, so the first person
# to register afterwards gets account #1.
#
# This exists because balances are derived by summing postings, and postings
# from before per-user accounts existed aren't owned by anyone -- left in
# place, they would silently become the opening balance of whoever happened to
# register into that account number.
#
#     python reset_ledger.py            # asks first
#     python reset_ledger.py --yes      # no prompt (for scripts)
#
# Reads DATABASE_URL the same way the app does, so point it at the same
# database the app is using.

import asyncio
import sys

from sqlalchemy import text

from database import engine

# audit_log and outbox both reference transactions, so this either needs the
# right order or CASCADE. CASCADE is used so the list stays correct even if
# someone adds another referencing table later.
TABLES = ["audit_log", "outbox", "processed_events", "postings", "transactions", "accounts", '"user"']


async def reset() -> None:
    async with engine.begin() as conn:
        counts_before = {}
        for table in TABLES:
            result = await conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            counts_before[table] = result.scalar()

        await conn.execute(text(f"TRUNCATE TABLE {', '.join(TABLES)} RESTART IDENTITY CASCADE"))

    print("Deleted:")
    for table, count in counts_before.items():
        print(f"  {table:20} {count}")
    print("\nDone. Ids restart at 1 -- the next user to register gets account #1.")


if __name__ == "__main__":
    if "--yes" not in sys.argv:
        print("This permanently deletes ALL users, accounts, and ledger history.")
        if input("Type 'reset' to continue: ").strip() != "reset":
            print("Aborted.")
            sys.exit(1)
    asyncio.run(reset())
