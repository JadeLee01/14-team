## Local Chat Test

1. Seed demo vector data (contract + optional corpus):
```bash
python3 scripts/seed_chat_demo_data.py --easy-contract-id 999001
```

2. Run API server:
```bash
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

3. Test chat SSE in another terminal:
```bash
python3 scripts/test_chat_sse.py --easy-contract-id 999001
```
