#!/usr/bin/env python3
"""
ASILO FINANZA CRASHMETER - VERSIONE HARDCORE DEFINITIVA
v1.1.2 (Fix Yahoo MultiIndex)

LOGICA: Floor fisso a 80 quando trend è ribassista.
"Non si afferra il coltello che cade" - Punto.

CHANGELOG v1.1.2:
- FIX CRITICO YAHOO: Gestione colonne MultiIndex (yfinance update)
  Ora lo script appiattisce le colonne se Yahoo restituisce il formato (Price, Ticker).
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
print("ASILO FINANZA CRASHMETER - HARDCORE EDITION (v1.1.2)")
print("=" * 60)

# =============================================================================
# FUNZIONE DI SUPPORTO PER YAHOO FIX
# =============================================================================
def clean_yahoo_cols(df):
    """
    Risolve il bug del MultiIndex di yfinance.
    Se le colonne sono ('Adj Close', '^GSPC'), le trasforma in 'Adj Close'.
    """
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

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
        # Scarica raw e pulisce le colonne
        raw = yf.download('^GSPC', period='max', progress=False)
        raw = clean_yahoo_cols(raw) # FIX v1.1.2
        
        price = raw['Adj Close']
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
        rates = rates.resample('ME').last()
        print(f"      ✓ {len(rates)} mesi da FRED")
        return rates
    except Exception as e:
        print(f"      ⚠ FRED fallito, provo Yahoo ^TNX...")
    
    # Tentativo 2: Yahoo
    try:
        raw_tnx = yf.download('^TNX', period='max', progress=False)
        raw_tnx = clean_yahoo_cols(raw_tnx) # FIX v1.1.2
        
        tnx = raw_tnx['Adj Close']
        tnx_monthly = tnx.resample('ME').last()
        
        ultimo_valore = tnx_monthly.iloc[-1]
        
        if ultimo_valore > 100:
            rates = tnx_monthly / 100 # Basis points
        elif ultimo_valore > 1:
            rates = tnx_monthly / 100 # Percentuale
        else:
            rates = tnx_monthly # Decimale
            
        print(f"      ✓ {len(rates)} mesi da Yahoo")
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

# =============================================================================
# 4. CRASHMETER HARDCORE
# =============================================================================

print("Applicazione logica HARDCORE (floor a 80)...\n")

df['Base_Risk'] = df['Valuation_Risk'] * 0.60 + df['Extension_Risk'] * 0.40
df['Base_Risk'] = df['Base_Risk'].clip(0, 1)

def asilo_crashmeter_hardcore(row):
    raw_score = row['Base_Risk'] * 100
    if row['Trend_Bull'] == 1:
        return raw_score
    else:
        return max(80.0, raw_score + 10)

df['CrashMeter'] = df.apply(asilo_crashmeter_hardcore, axis=1)
df['CrashMeter_Smooth'] = df['CrashMeter'].rolling(3, min_periods=1).mean()

# =============================================================================
# 5. OUTPUT E RISULTATI
# =============================================================================

last = df.iloc[-1]
score = last['CrashMeter_Smooth']

if score > 80:
    zona = "🔴 ZONA ROSSA ESTREMA"
    azione = "Proteggere il capitale. Ridurre esposizione azionaria."
elif score > 50:
    zona = "🟡 ZONA GIALLA (CAUTELA)"
    azione = "Accumulo graduale solo su debolezze significative."
else:
    zona = "🟢 ZONA VERDE"
    azione = "Ok sovrappesare azioni per il lungo periodo."

print("=" * 60)
print(f"ASILO FINANZA CRASHMETER - {df.index[-1].strftime('%B %Y').upper()}")
print("=" * 60)
print(f"🎯 SCORE: {score:.1f}/100 ({zona})")

# =============================================================================
# 6. SALVATAGGIO FILE PER WORDPRESS
# =============================================================================

json_out = {
    "ultimo_aggiornamento": df.index[-1].strftime("%d %B %Y"),
    "score": round(score, 1),
    "zona": "ROSSA" if score > 80 else "GIALLA" if score > 50 else "VERDE",
    "colore_hex": "#d32f2f" if score > 80 else "#fbc02d" if score > 50 else "#388e3c",
    "emoji": "🔴" if score > 80 else "🟡" if score > 50 else "🟢",
    "cape_attuale": round(last['CAPE'], 1),
    "cape_percentile": int(last['CAPE_Percentile'] * 100),
    "valuation_risk_percentile": int(last['Valuation_Risk'] * 100),
    "ecy_percent": round(last['ECY'] * 100, 2),
    "distanza_trend_percent": round(last['Extension'] * 100, 1),
    "trend": "RIALZISTA" if last['Trend_Bull'] == 1 else "RIBASSISTA",
    "azione_consigliata": azione,
    "timestamp_unix": int(df.index[-1].timestamp()),
    "versione": "Hardcore 1.1.2"
}

with open('crashmeter_status.json', 'w', encoding='utf-8') as f:
    json.dump(json_out, f, ensure_ascii=False, indent=2)
print("✓ JSON salvato")

df[['CrashMeter_Smooth', 'CAPE', 'Extension', 'Trend_Bull']].tail(36).to_csv('crashmeter_history.csv')
print("✓ CSV salvato")

# =============================================================================
# 7. GRAFICO STORICO
# =============================================================================

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), height_ratios=[2, 1])

# Pannello 1
ax1.fill_between(df.index, 80, 100, color='#d32f2f', alpha=0.3)
ax1.fill_between(df.index, 50, 80, color='#fbc02d', alpha=0.3)
ax1.fill_between(df.index, 0, 50, color='#388e3c', alpha=0.3)
ax1.plot(df.index, df['CrashMeter_Smooth'], color='darkred', linewidth=2.5)
ax1.axhline(80, color='red', linestyle='--', alpha=0.5)
ax1.axhline(50, color='orange', linestyle='--', alpha=0.5)
ax1.set_ylim(0, 100)
ax1.set_title(f"Asilo Finanza CrashMeter → {score:.1f}/100 ({zona})", fontsize=16, fontweight='bold')
ax1.grid(alpha=0.3)

# Pannello 2
ax2_cape = ax2.twinx()
ax2.bar(df.index, df['Trend_Bull'], color='lightblue', alpha=0.3)
ax2_cape.plot(df.index, df['CAPE'], color='navy', linewidth=1.5)
ax2_cape.axhline(df['CAPE'].median(), color='gray', linestyle='--', alpha=0.5)
ax2.set_ylabel("Trend (Bull=1)", fontweight='bold')
ax2_cape.set_ylabel("CAPE", fontweight='bold', color='navy')
plt.tight_layout()
plt.savefig('crashmeter_grafico.png', dpi=300, bbox_inches='tight')
print("✓ PNG salvato")
