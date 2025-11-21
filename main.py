#!/usr/bin/env python3
"""
ASILO FINANZA CRASHMETER - VERSIONE HARDCORE DEFINITIVA
v1.1.1 (21 Novembre 2025) - Triple-Reviewed Edition

LOGICA: Floor fisso a 80 quando trend è ribassista.
"Non si afferra il coltello che cade" - Punto.

REVIEW PROCESS:
- Round 1: Gemini & Grok (logica Hardcore)
- Round 2: ChatGPT (fix allineamento date + percentili)
- Round 3: Grok (validazione finale)
- Implementazione: Claude (Anthropic)

CHANGELOG v1.1.1:
- Fix critico: allineamento date a fine mese [ChatGPT]
- Fix critico: FRED rates resample mensile [ChatGPT]
- Fix naming: percentile CAPE separato [ChatGPT]
- Miglioramento: Yahoo ^TNX multi-scenario [Grok/Claude]
- Confermato: ensure_ascii JSON [Grok]

AUTORI:
- Manuel (Asilo Finanza)
- Claude (Anthropic)
- Grok (xAI)
- Gemini (Google)
- ChatGPT (OpenAI)
"""

import pandas as pd
import numpy as np
import yfinance as yf
import pandas_datareader.data as web
import requests
import io
import matplotlib.pyplot as plt
import json
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

print("=" * 60)
print("ASILO FINANZA CRASHMETER - HARDCORE EDITION")
print("=" * 60)

# =============================================================================
# 1. SCARICAMENTO DATI CON GESTIONE ERRORI ROBUSTA
# =============================================================================

def get_shiller_cape():
    """Scarica CAPE da Yale (Shiller)"""
    url = "http://www.econ.yale.edu/~shiller/data/ie_data.xls"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    print("\n[1/3] Scarico dati Shiller CAPE...")
    try:
        response = requests.get(url, headers=headers, timeout=10)
        df = pd.read_excel(io.BytesIO(response.content), sheet_name="Data", header=7)
        
        # Parsing date robusto - ALLINEATO A FINE MESE
        df['Date'] = df['Date'].astype(str).str.replace('.', '', regex=False)
        df = df[df['Date'].str.len() >= 6]
        df['Date'] = pd.to_datetime(df['Date'].str[:6], format='%Y%m') + pd.offsets.MonthEnd(0)
        df.set_index('Date', inplace=True)
        
        cape = pd.to_numeric(df['CAPE'], errors='coerce').dropna()
        print(f"      ✓ {len(cape)} mesi di dati CAPE (da {cape.index[0].year})")
        return cape
        
    except Exception as e:
        print(f"      ✗ ERRORE: {e}")
        raise

def get_market_price():
    """Scarica S&P 500 da Yahoo"""
    print("\n[2/3] Scarico prezzi S&P 500 (^GSPC)...")
    try:
        price = yf.download('^GSPC', period='max', progress=False)['Adj Close']
        price_monthly = price.resample('ME').last()
        print(f"      ✓ {len(price_monthly)} mesi di prezzi")
        return price_monthly
    except Exception as e:
        print(f"      ✗ ERRORE: {e}")
        raise

def get_rates_robust():
    """Scarica tassi US 10Y (FRED → Yahoo → Fallback)"""
    print("\n[3/3] Scarico tassi US 10Y...")
    
    # Tentativo 1: FRED
    try:
        rates = web.DataReader('GS10', 'fred', '1950-01-01')['GS10'] / 100
        rates = rates.resample('ME').last()  # ← AGGIUNTO: allineamento a fine mese
        print(f"      ✓ {len(rates)} mesi da FRED")
        return rates
    except Exception as e:
        print(f"      ⚠ FRED fallito, provo Yahoo ^TNX...")
    
    # Tentativo 2: Yahoo
    try:
        tnx = yf.download('^TNX', period='max', progress=False)['Adj Close']
        tnx_monthly = tnx.resample('ME').last()
        
        # ^TNX dovrebbe essere sempre in basis points (es. 418.0 = 4.18%)
        # Ma gestiamo diversi formati possibili per robustezza
        ultimo_valore = tnx_monthly.iloc[-1]
        
        if ultimo_valore > 100:
            # Formato basis points (es. 418 → 4.18%)
            rates = tnx_monthly / 100
            print(f"      ✓ {len(rates)} mesi da Yahoo (diviso per 100 da basis points)")
        elif ultimo_valore > 1:
            # Formato percentuale (es. 4.18 → 4.18%)
            rates = tnx_monthly / 100
            print(f"      ✓ {len(rates)} mesi da Yahoo (diviso per 100)")
        else:
            # Formato decimale (es. 0.0418 → 4.18%)
            rates = tnx_monthly
            print(f"      ✓ {len(rates)} mesi da Yahoo (già in decimale)")
        
        return rates
    except Exception as e:
        print(f"      ✗ ERRORE CRITICO: {e}")
        raise

# =============================================================================
# 2. MERGE E VALIDAZIONE DATI
# =============================================================================

cape = get_shiller_cape()
price = get_market_price()
rates = get_rates_robust()

df = pd.concat([price, rates, cape], axis=1).dropna()
df.columns = ['Price', 'US10Y', 'CAPE']

print(f"\n{'='*60}")
print(f"Dataset finale: {len(df)} mesi ({df.index[0].strftime('%Y-%m')} → {df.index[-1].strftime('%Y-%m')})")
print(f"{'='*60}")

# Validazione
if len(df) < 120:
    raise ValueError(f"⚠ Dati insufficienti: {len(df)} mesi (servono almeno 120)")

if df['CAPE'].iloc[-1] < 5 or df['CAPE'].iloc[-1] > 100:
    print(f"⚠ WARNING: CAPE anomalo ({df['CAPE'].iloc[-1]:.1f})")

if df['US10Y'].iloc[-1] < 0 or df['US10Y'].iloc[-1] > 0.20:
    print(f"⚠ WARNING: Tasso anomalo ({df['US10Y'].iloc[-1]*100:.2f}%)")

# =============================================================================
# 3. CALCOLO INDICATORI
# =============================================================================

print("\nCalcolo indicatori...")

# A. VALUATION RISK (Excess CAPE Yield)
df['EY'] = 1 / df['CAPE']  # Earnings Yield
df['ECY'] = df['EY'] - df['US10Y']  # Excess CAPE Yield (Yield Gap)
df['Valuation_Risk'] = (-df['ECY']).expanding(min_periods=120).rank(pct=True)

# Calcolo VERO percentile CAPE per il JSON
df['CAPE_Percentile'] = df['CAPE'].expanding(min_periods=120).rank(pct=True)

# B. EXTENSION RISK (Distance from SMA10)
df['SMA10'] = df['Price'].rolling(10).mean()
df['Extension'] = df['Price'] / df['SMA10'] - 1
df['Extension_Risk'] = df['Extension'].expanding(min_periods=120).rank(pct=True)

# C. TREND FILTER
df['Trend_Bull'] = (df['Price'] > df['SMA10']).astype(int)

# Drop righe senza SMA10 (prime 9 osservazioni)
df = df.dropna(subset=['SMA10'])
print(f"   Dataset pulito: {len(df)} mesi con tutti gli indicatori")

# =============================================================================
# 4. CRASHMETER HARDCORE
# =============================================================================

print("Applicazione logica HARDCORE (floor a 80)...\n")

df['Base_Risk'] = df['Valuation_Risk'] * 0.60 + df['Extension_Risk'] * 0.40
df['Base_Risk'] = df['Base_Risk'].clip(0, 1)  # Safety clamp

def asilo_crashmeter_hardcore(row):
    """
    LOGICA HARDCORE DI ASILO FINANZA:
    - Se trend positivo (prezzo > SMA10) → usa il punteggio calcolato
    - Se trend negativo (prezzo < SMA10) → FLOOR FISSO A 80
        Messaggio: "Non afferrare il coltello che cade"
    """
    raw_score = row['Base_Risk'] * 100
    
    if row['Trend_Bull'] == 1:
        # Trend rialzista: vai normale
        return raw_score
    else:
        # Trend ribassista: FLOOR HARDCORE
        return max(80.0, raw_score + 10)

df['CrashMeter'] = df.apply(asilo_crashmeter_hardcore, axis=1)
df['CrashMeter_Smooth'] = df['CrashMeter'].rolling(3, min_periods=1).mean()

# =============================================================================
# 5. OUTPUT E RISULTATI
# =============================================================================

last = df.iloc[-1]
score = last['CrashMeter_Smooth']

print("=" * 60)
print(f"ASILO FINANZA CRASHMETER - {df.index[-1].strftime('%B %Y').upper()}")
print("=" * 60)

print(f"\n🎯 SCORE FINALE: {score:.1f}/100")

if score > 80:
    zona = "🔴 ZONA ROSSA ESTREMA"
    azione = "Proteggere il capitale. Ridurre esposizione azionaria."
elif score > 50:
    zona = "🟡 ZONA GIALLA (CAUTELA)"
    azione = "Accumulo graduale solo su debolezze significative."
else:
    zona = "🟢 ZONA VERDE"
    azione = "Ok sovrappesare azioni per il lungo periodo."

print(f"📊 STATO: {zona}")
print(f"\n📈 METRICHE CHIAVE:")
print(f"   • CAPE: {last['CAPE']:.1f} ({int(last['CAPE_Percentile']*100)}° percentile storico)")
print(f"   • Excess CAPE Yield: {last['ECY']*100:+.2f}% ({int(last['Valuation_Risk']*100)}° percentile rischio)")
print(f"   • Distanza da SMA10: {last['Extension']*100:+.1f}%")
print(f"   • Trend: {'RIALZISTA ⬆' if last['Trend_Bull'] == 1 else 'RIBASSISTA ⬇'}")
print(f"\n💡 AZIONE CONSIGLIATA:")
print(f"   {azione}")
print("=" * 60)

# =============================================================================
# 6. SALVATAGGIO FILE PER WORDPRESS
# =============================================================================

# JSON per il sito web
json_out = {
    "ultimo_aggiornamento": df.index[-1].strftime("%d %B %Y"),
    "score": round(score, 1),
    "zona": "ROSSA" if score > 80 else "GIALLA" if score > 50 else "VERDE",
    "colore_hex": "#d32f2f" if score > 80 else "#fbc02d" if score > 50 else "#388e3c",
    "emoji": "🔴" if score > 80 else "🟡" if score > 50 else "🟢",
    
    # Metriche chiave
    "cape_attuale": round(last['CAPE'], 1),
    "cape_percentile": int(last['CAPE_Percentile'] * 100),  # ← CORRETTO: vero percentile CAPE
    "valuation_risk_percentile": int(last['Valuation_Risk'] * 100),  # ← AGGIUNTO: percentile ECY
    "ecy_percent": round(last['ECY'] * 100, 2),
    "distanza_trend_percent": round(last['Extension'] * 100, 1),
    "trend": "RIALZISTA" if last['Trend_Bull'] == 1 else "RIBASSISTA",
    
    # Azione consigliata
    "azione_consigliata": azione,
    
    # Timestamp per cache
    "timestamp_unix": int(df.index[-1].timestamp()),
    "versione": "Hardcore 1.0"
}

with open('crashmeter_status.json', 'w', encoding='utf-8') as f:
    json.dump(json_out, f, ensure_ascii=False, indent=2)

print("\n✓ File salvato: crashmeter_status.json")

# CSV degli ultimi 36 mesi per grafici WordPress
df_export = df[['CrashMeter_Smooth', 'CAPE', 'Extension', 'Trend_Bull']].tail(36)
df_export.to_csv('crashmeter_history.csv')
print("✓ File salvato: crashmeter_history.csv")

# =============================================================================
# 7. GRAFICO STORICO
# =============================================================================

print("\nGenerazione grafico...")

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), height_ratios=[2, 1])

# Pannello 1: CrashMeter
ax1.fill_between(df.index, 80, 100, color='#d32f2f', alpha=0.3, label='Zona Rossa')
ax1.fill_between(df.index, 50, 80, color='#fbc02d', alpha=0.3, label='Zona Gialla')
ax1.fill_between(df.index, 0, 50, color='#388e3c', alpha=0.3, label='Zona Verde')

ax1.plot(df.index, df['CrashMeter_Smooth'], color='darkred', linewidth=2.5, label='CrashMeter')

ax1.axhline(80, color='red', linestyle='--', alpha=0.5)
ax1.axhline(50, color='orange', linestyle='--', alpha=0.5)

ax1.set_ylim(0, 100)
ax1.set_title(f"Asilo Finanza CrashMeter → {score:.1f}/100 ({zona})", 
              fontsize=16, fontweight='bold', pad=20)
ax1.set_ylabel("Score (0-100)", fontweight='bold')
ax1.legend(loc='upper left')
ax1.grid(alpha=0.3)

# Pannello 2: CAPE
ax2_cape = ax2.twinx()
ax2.bar(df.index, df['Trend_Bull'], color='lightblue', alpha=0.3, label='Trend Bullish')
ax2_cape.plot(df.index, df['CAPE'], color='navy', linewidth=1.5, label='CAPE')
ax2_cape.axhline(df['CAPE'].median(), color='gray', linestyle='--', alpha=0.5, label='Mediana storica')

ax2.set_ylabel("Trend (1=Bull, 0=Bear)", fontweight='bold')
ax2_cape.set_ylabel("CAPE", fontweight='bold', color='navy')
ax2_cape.tick_params(axis='y', labelcolor='navy')

ax2.set_xlabel("Anno", fontweight='bold')

ax2.legend(loc='upper left')
ax2_cape.legend(loc='upper right')
ax2.grid(alpha=0.3)

plt.tight_layout()
plt.savefig('crashmeter_grafico.png', dpi=300, bbox_inches='tight')
print("✓ File salvato: crashmeter_grafico.png")

print("\n" + "=" * 60)
print("COMPLETATO! File pronti per upload WordPress:")
print("  1. crashmeter_status.json → leggi con PHP")
print("  2. crashmeter_history.csv → grafici dinamici")
print("  3. crashmeter_grafico.png → immagine per articoli")
print("=" * 60)
