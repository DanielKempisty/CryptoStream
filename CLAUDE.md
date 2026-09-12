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
    → Kafka (3 brokery, KRaft) — topic wejściowy (surowe trades)
    → Flink SQL (parsing, tabelaryzacja, agregacje)
    → Kafka — topic wyjściowy (dane przetworzone)
    → Kafka Connect
        → Elasticsearch → Kibana (dashboard near real-time)
        → Postgres (dane stabelaryzowane, analizy historyczne)
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
| Przetwarzanie strumieniowe | Flink SQL (PyFlink) | 2.2.1 |
| Notebook do Flink SQL | JupyterLab + PyFlink | apache-flink==2.2.1 |
| Dostawa do sinków | Kafka Connect | TBD |
| Baza relacyjna | PostgreSQL | 16.10 |
| Wyszukiwanie / dashboard | Elasticsearch + Kibana | 9.5.2 |
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

8. **Każda zmiana — kodu, plików, konfiguracji, wpisów w Notion —
   wymaga mojej zgody zanim zostanie wykonana.** Zawsze najpierw
   przedstaw propozycję (diff / treść) i poczekaj na potwierdzenie,
   nawet dla drobnych rzeczy.

## Planowanie i notatki (Notion)

Plan projektu (TODO) i notatki z nauki prowadzone są w Notion, nie tu:
- Plan/TODO: https://app.notion.com/p/3ac34a300513802d8ae4ebb7927f9dfa
- Notatki: https://app.notion.com/p/3ac34a30051380cd99a8c50356048dc2

Zasady:
- Claude aktualizuje TODO i notatki **tylko za zgodą autora** — zawsze
  proponuje treść przed zapisem.
- Claude pilnuje zgodności bieżącej pracy z planem w Notion i zwraca
  uwagę, gdy praca odjeżdża od planu bez podjętej decyzji.
- Claude może przypominać o zapisaniu notatki, gdy pojawi się
  nietrywialna decyzja/nauka, nawet jeśli autor o tym nie poprosi.
- **Pod żadnym pozorem Claude nie usuwa niczego w Notion** — żadnych
  notatek, punktów TODO, sekcji, podstron. Usuwanie treści z Notion
  robi wyłącznie autor, ręcznie. Dotyczy to też pośredniego usuwania
  (np. nadpisania treści, które usunęłoby coś istniejącego) —
  dozwolone jest wyłącznie dopisywanie/edycja za zgodą, nigdy kasowanie.

### Styl notatek

W Notion współistnieją dwa różne gatunki notatek — nowe notatki mają
naśladować ten sam wzorzec, nie wymyślać nowego formatu.

**A. Notatki decyzyjne** (jedna notatka toggle na temat/komponent,
np. "Postgres w Dockerze", "Kafka w Dockerze") — struktura:
1. Cel/Kontekst — co i po co się robi
2. Decyzja — z uzasadnieniem; jeśli były rozważane alternatywy, krótko
   dlaczego odrzucone
3. Konfiguracja — pełny, działający fragment kodu/configu
4. Wyjaśnienie kluczowych elementów — linijka po linijce, co dana
   opcja robi i **dlaczego** akurat tak, nie tylko co robi
5. (jeśli wystąpił błąd) Napotkany problem — dokładny tekst błędu →
   Diagnoza (przyczyna) → Rozwiązanie krok po kroku
6. Weryfikacja — jak sprawdzić, że faktycznie działa
7. Wniosek na przyszłość / Do zapamiętania — uogólniona lekcja, nie
   tylko podsumowanie kroków

Dodatkowo: tabele przy porównaniach dwóch podejść, analogie przy
trudnych konceptach, świadome odniesienia do wcześniej poznanych
narzędzi (np. "dlaczego Postgres tego nie potrzebuje, a Kafka tak").

**B. Crash-course'y** (samouczek do nowego narzędzia, ponumerowany
program, np. "Kafka crash-course" → moduły 1.1, 1.2, ...) — struktura
per moduł:
1. Teoria — krótkie wprowadzenie konceptu
2. Komenda/przykład z wyjaśnieniem każdej flagi/opcji
3. Do zapamiętania — kluczowy wniosek modułu
4. Zadania — 2-3 ćwiczenia do samodzielnego rozwiązania, często
   każące porównać zachowanie z wcześniej poznanym narzędziem
5. Rozwiązania i wyjaśnienia — nie tylko komenda-odpowiedź, ale pełna
   analiza (szczególnie gdy ćwiczenie odsłania jakąś pułapkę/gotchę)

Wspólne dla obu gatunków: notatki po polsku, nazwy narzędzi/komend/
identyfikatorów/kluczy configu zostają w oryginale (angielski, inline
code); pogrubienie na kluczowych terminach; toggle jako podstawowy
blok organizacyjny (H1 = temat, zagnieżdżone nagłówki = podtematy).

### Formatowanie (składnia Notion)

- **Temat notatki = toggle heading H1:** `# Tytuł notatki {toggle="true"}`.
  Każdy crash-course to też jeden taki toggle (`# Kafka crash-course {toggle="true"}`).
- **Podsekcje zagnieżdżone przez wcięcie (tab)** pod nagłówkiem nadrzędnym.
  Dłuższe/bardziej złożone notatki (np. "Kafka w Dockerze") zagnieżdżają
  same podsekcje jako kolejne toggle heading: `## Nazwa podsekcji {toggle="true"}`
  — dzięki temu notatka jest zwijalna po sekcjach, nie trzeba scrollować
  całości. Krótsze notatki (np. "Środowisko Pythona") używają zwykłych,
  nie-zwijanych nagłówków `## Nazwa` dla tych samych podsekcji (Problem,
  Decyzja, Diagnostyka, Rozwiązanie) — bo krótka notatka nie potrzebuje
  dodatkowej warstwy zwijania.
- **Trzeci poziom** (`### Nazwa`) dla podpunktów wewnątrz podsekcji
  (np. "### Kluczowe elementy", "### Przez terminal" vs "### Przez VS Code"),
  zwykle bez toggle — to już najniższy poziom, nie ma co dalej zwijać.
- **`---` (divider)** między sąsiednimi toggle-podsekcjami tego samego
  poziomu, dla wizualnego oddechu.
- **Bloki kodu** zawsze z językiem: ```bash, ```sql, ```yaml, ```python,
  a output/błędy komend jako ```plain text — nigdy goły blok bez języka.
- **Tabele** przy porównaniach dwóch-trzech wariantów (kolumny = warianty
  lub cechy, wiersze = druga oś porównania) — nie prozą, gdy da się to
  zestawić w tabeli.
- **`<br>` zamiast zwykłego entera** w środku akapitu (żeby nie rozbijać
  jednego bloku tekstu na kilka oddzielnych bloków w Notion) — dotyczy
  tylko wielolinijkowych akapitów prozy, nie bloków kodu.
- **Pogrubienie** (`**tekst**`) na pierwszym wprowadzeniu kluczowego
  terminu/nazwy opcji w danym akapicie, nie na całych zdaniach.
- **Inline code** (`` `tekst` ``) na każdej nazwie pliku, komendzie,
  kluczu configu, wersji, identyfikatorze — nigdy zwykłym tekstem.

## Konwencje projektu

- **Branche:** `typ/krótki-opis` (kebab-case), np. `feature/kafka-producer`,
  `fix/websocket-reconnect`, `docs/architecture-diagram`, `chore/docker-setup`
- **Commity:** [Conventional Commits](https://www.conventionalcommits.org/) —
  `feat: add kafka producer for binance trade stream`
- **Branch główny:** `main` — powinien zawsze być w stanie działającym
- **Wolumeny Docker:** named volumes dla danych baz (Postgres, Kafka,
  Elasticsearch), nie bind mounty — unika problemów z uprawnieniami plików
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
- Flink pisze wynik na wyjściowy topic Kafki, nie bezpośrednio do
  Elasticsearch/Postgresa. Fan-out do obu baz robi Kafka Connect
  (nie Flink). Powód: oficjalny konektor Elasticsearch dla Flinka
  jest utrzymywany tylko do wersji ES 7.x, więc każda nowsza wersja
  ES wymagałaby albo cofania jej do EOL-owanej wersji, albo pisania
  własnego sinka — Kafka Connect ma aktywnie utrzymywany konektor ES
  i rozwiązuje to bez kompromisów wersyjnych. Flink SQL dalej robi
  100% przetwarzania (parsing, agregacje) — zmienia się tylko
  mechanizm dostawy wyniku.
- MinIO/data lake świadomie odłożone na "rozszerzenia po MVP"
