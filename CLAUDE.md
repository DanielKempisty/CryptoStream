# CLAUDE.md

Ten plik daje Claude Code kontekst projektu. Czytaj go na początku
każdej sesji w tym repo.

## O projekcie

**Nazwa:** Real-Time Crypto Market Analytics Pipeline

**Cel:** Portfolio project demonstrujący budowę end-to-end pipeline'u
streamingowego. Dashboard do analizy rynku kryptowalut w czasie
rzeczywistym (przydatny dla traderów), zbudowany żeby przejść od
wiedzy koncepcyjnej do praktycznej umiejętności data engineeringu.

**Kontekst zawodowy:** Autor przechodzi z roli analitycznej do pracy
jako Senior Specialist ds. bezpieczeństwa usług płatniczych (fraud/
payment security) w banku. Zna podstawy Kafki, Dockera, Flink SQL
(architektura, proste zapytania), ale nigdy nie budował end-to-end
pipeline'u samodzielnie.

## Architektura

```
Producer (Python, Binance WebSocket)
    → Kafka (3 brokery, KRaft)
    → Flink SQL (parsing, tabelaryzacja, agregacje)
        → Elasticsearch → Kibana (dashboard near real-time)
        → SQL Server / Postgres (dane stabelaryzowane, analizy historyczne)
```

Całość w Docker Compose, jeden plik dla całego stacku.

## Zakres MVP

- Jeden symbol: BTC/USDT
- Jeden typ zdarzenia: strumień `trade` z Binance WebSocket
- Cel: przejść pełny pipeline end-to-end, zanim dojdzie się do
  rozszerzeń (więcej symboli, enrichment, MinIO jako data lake)

## Stack technologiczny

| Warstwa | Technologia | Wersja (przypięta) |
|---|---|---|
| Źródło danych | Binance WebSocket API | — |
| Producent | Python | 3.12.7 (przez pyenv) |
| Message broker | Apache Kafka (KRaft, 3 brokery) | 4.3.1 |
| UI do Kafki | Kafbat UI | v1.5.0 |
| Przetwarzanie strumieniowe | Flink SQL (PyFlink) | TBD |
| Baza relacyjna | PostgreSQL | 16.10 |
| Wyszukiwanie / dashboard | Elasticsearch + Kibana | TBD |
| Konteneryzacja | Docker Compose | — |

## KRYTYCZNE ZASADY PRACY — PRZECZYTAJ PRZED KAŻDĄ SESJĄ

To jest projekt **edukacyjny**. Autor uczy się przez samodzielne
wykonywanie pracy, nie przez otrzymywanie gotowych rozwiązań.
Poniższe zasady są nadrzędne wobec zwykłej pomocności:

1. **Nigdy nie pisz kodu za mnie i nie edytuj plików samodzielnie.**
   Zawsze pracuj w **Plan Mode** — proponuj podejście, wyjaśnij
   koncept, pokaż przykładowy fragment kodu/konfiguracji jako
   ilustrację (nie jako gotowiec do wklejenia), ale finalne wpisanie
   kodu do plików zawsze zostaw mnie.

2. **Tryb wyjaśniania, nie wykonywania.** Opisz koncept, wytłumacz
   "dlaczego", pokaż przykład — ale nie wykonuj zmian w repo bez
   wyraźnej prośby.

3. **Gdy utknę na błędzie, pytaj najpierw, zanim podasz rozwiązanie:**
   "co widzisz w logu?", "co Twoim zdaniem może być przyczyną?".
   Celem jest ćwiczenie diagnozowania, nie tylko dostawanie gotowych
   odpowiedzi. Dawaj realną szansę na samodzielną diagnozę przed
   podaniem fixu.

4. **Infrastruktura (Docker, docker-compose) — mniejszy nacisk na
   samodzielne pisanie od zera.** Autor świadomie zdecydował, że
   ręczne konfigurowanie klastrów w Dockerze rzadko przydaje się
   w pracy (zazwyczaj środowiska są gotowe) — tu można podawać
   gotowe przykłady konfiguracji, pod warunkiem że dobrze wytłumaczone
   (co robi każda linijka i dlaczego).

5. **Flink SQL, logika przetwarzania danych, kod aplikacyjny — tu
   priorytet na samodzielną pracę.** To są umiejętności, które
   faktycznie przekładają się na rolę zawodową — zachęcaj do
   samodzielnych prób przed podaniem przykładu.

6. **Zawsze przypinaj konkretne wersje obrazów Docker** (nigdy
   `latest`) — sprawdź aktualną, stabilną wersję, jeśli nieznana.

7. **Sekrety zawsze przez `.env`**, dodany do `.gitignore`, z
   `.env.example` jako dokumentacją struktury (bez realnych wartości).

## Konwencje projektu

- **Branche:** `typ/krótki-opis` (kebab-case), np. `feature/kafka-producer`,
  `fix/websocket-reconnect`, `docs/architecture-diagram`, `chore/docker-setup`
- **Commity:** [Conventional Commits](https://www.conventionalcommits.org/) —
  `feat: add kafka producer for binance trade stream`
- **Branch główny:** `main` — powinien zawsze być w stanie działającym
- **Wolumeny Docker:** named volumes dla danych baz (Postgres, Kafka,
  SQL Server), nie bind mounty — unika problemów z uprawnieniami plików
- **Środowisko Python:** `pyenv local` (wersja projektu) + `venv`
  (izolacja zależności), w tej kolejności

## Struktura repo (do aktualizacji w miarę rozwoju)

```
CRYPTO_STREAM_PROJECT/
├── docker-compose.yml
├── .env                    (nie w Git)
├── .env.example
├── README.md
├── CLAUDE.md               (ten plik)
└── ...
```

## Znane decyzje architektoniczne (nie renegocjować bez wyraźnej prośby)

- KRaft zamiast Zookeeper (Kafka 4.0+ nie wspiera już Zookeepera)
- Trzy brokery Kafki (nie jeden, nie więcej) — mikro-klaster do nauki
  replikacji/partycjonowania
- Fan-out z Flinka do dwóch sinków (Elasticsearch + Postgres/SQL
  Server), nie osobny konsument z Kafki bezpośrednio do bazy
- MinIO/data lake świadomie odłożone na "rozszerzenia po MVP"
