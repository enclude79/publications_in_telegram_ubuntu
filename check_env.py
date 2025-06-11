with open('.env', 'r', encoding='utf-8') as f:
    for i, line in enumerate(f, 1):
        print(f"{i:02d}: {repr(line)}") 