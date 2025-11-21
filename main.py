import html
import json
import math
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode, quote_plus

import pandas as pd
import psycopg2
import requests
import streamlit as st
from dotenv import load_dotenv
from google.cloud import translate as translate_client

TAG_RE = re.compile(r"<[^>]+>")
PROPERTY_KEYWORDS = {
    "Townhouse": ["ทาวน์", "townhome", "town house", "townhouse"],
    "Single House": ["บ้านเดี่ยว", "single", "detached"],
    "Condo": ["คอนโด", "condo", "คอนโดมิเนียม"],
    "Commercial": ["อาคารพาณิชย์", "commercial", "office", "อาคาร", "ตึก"],
    "Land": ["ที่ดิน", "land", "plot"],
}

class JsonDeckSpec:
    def __init__(self, spec):
        self._spec = spec

    def to_json(self):
        return json.dumps(self._spec)
 
PHOTO_AVOID_KEYWORDS = [
    "map",
    "แผนที่",
    "direction",
    "route",
    "floor",
    "layout",
    "plan",
    "googleapis",
    "staticmap",
]

PHOTO_FAVOR_KEYWORDS = [
    "house",
    "home",
    "front",
    "exterior",
    "facade",
    "villa",
    "living",
    "interior",
]

st.markdown(
    """
    <style>
    .stat-slab {
        display: flex;
        gap: 12px;
    }

    .stat-card {
        flex: 1;
        background: rgba(255,255,255,0.9);
        border-radius: 24px;
        padding: 16px 18px;
        border: 1px solid rgba(255,255,255,0.7);
        text-align: left;
        box-shadow: 0 16px 30px rgba(15,23,42,0.1);
    }

    .stat-card label {
        display: block;
        font-size: 11px;
        letter-spacing: 0.2em;
        text-transform: uppercase;
        color: rgba(15,23,42,0.55);
    }

    .stat-card strong {
        display: block;
        font-size: 24px;
        margin-top: 4px;
        color: #0F172A;
    }

    .inline-login-card {
        border-radius: 26px;
        padding: 16px 18px;
        background: rgba(255,255,255,0.95);
        border: 1px solid rgba(15,23,42,0.05);
        box-shadow: 0 14px 32px rgba(15,23,42,0.08);
    }

    .inline-login-card h4 {
        margin: 0 0 2px;
        font-size: 18px;
        color: #0F172A;
    }

    .inline-login-card p {
        margin: 0 0 16px;
        font-size: 14px;
        color: rgba(15,23,42,0.6);
    }

    .inline-login-card .cta-row {
        display: flex;
        gap: 10px;
    }

    .inline-login-card .cta-row .stButton > button {
        width: 100%;
        border-radius: 18px;
        height: 48px;
        font-weight: 600;
        box-shadow: 0 16px 26px rgba(15,23,42,0.12);
    }

    .inline-login-card .cta-row .stButton > button[data-testid="baseButton-primary"],
    .inline-login-card .cta-row .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #1AAE80, #26C589);
        color: #fff;
        border: none;
    }

    .recommend-title {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin: 34px 0 12px;
    }

    .recommend-title h3 {
        margin: 0;
        font-size: 22px;
    }

    .recommend-cards {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
        gap: 18px;
    }

    .recommend-card {
        border-radius: 32px;
        padding: 16px;
        background: #fff;
        border: 1px solid rgba(15,23,42,0.05);
        box-shadow: 0 18px 36px rgba(15,23,42,0.12);
        display: flex;
        flex-direction: column;
        gap: 14px;
        text-decoration: none;
        color: #0F172A;
    }

    .recommend-media {
        position: relative;
        border-radius: 26px;
        overflow: hidden;
    }

    .recommend-media img {
        width: 100%;
        height: 190px;
        object-fit: cover;
        display: block;
    }

    .card-badge {
        position: absolute;
        top: 14px;
        left: 14px;
        padding: 6px 14px;
        border-radius: 999px;
        background: rgba(255,255,255,0.9);
        font-size: 13px;
        font-weight: 600;
        color: #0F172A;
        box-shadow: 0 10px 18px rgba(15,23,42,0.12);
    }

    .recommend-heart {
        position: absolute;
        top: 14px;
        right: 14px;
        width: 44px;
        height: 44px;
        border-radius: 50%;
        background: rgba(255,255,255,0.95);
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 20px;
        color: #d946ef;
        box-shadow: 0 12px 24px rgba(15,23,42,0.18);
    }

    .recommend-heart.saved {
        background: linear-gradient(135deg, #EC4899, #F97316);
        color: #fff;
    }

    .recommend-body {
        display: flex;
        flex-direction: column;
        gap: 10px;
    }

    .card-headline {
        display: flex;
        justify-content: space-between;
        gap: 12px;
        align-items: center;
    }

    .card-headline h4 {
        margin: 0;
        font-size: 20px;
    }

    .card-headline p {
        margin: 0;
        color: rgba(15,23,42,0.55);
        font-size: 14px;
    }

    .rec-price {
        font-size: 20px;
        color: #0F172A;
        white-space: nowrap;
    }

    .card-meta-row {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
    }

    .meta-pill {
        border-radius: 999px;
        background: rgba(15,23,42,0.05);
        padding: 6px 12px;
        font-size: 13px;
        color: rgba(15,23,42,0.75);
        display: inline-flex;
        align-items: center;
        gap: 4px;
    }

    .bottom-nav {
        margin-top: 28px;
        padding: 12px 18px;
        border-radius: 28px;
        background: rgba(255,255,255,0.95);
        border: 1px solid rgba(15,23,42,0.06);
        box-shadow: 0 18px 36px rgba(15,23,42,0.12);
    }

    .bottom-nav-grid {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 4px;
    }

    .bottom-nav-grid .stButton > button {
        width: 100%;
        border-radius: 18px;
        padding: 12px 0 8px;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        gap: 4px;
        background: transparent;
        border: none;
        color: rgba(15,23,42,0.6);
        font-weight: 600;
    }

    .bottom-nav-grid .stButton > button[data-testid="baseButton-primary"],
    .bottom-nav-grid .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #0F172A, #1F2650);
        color: #fff;
    }
        --surface-alt: #F7FCFA;
        --panel: #FFFFFF;
        --border: #D9E8E1;
        --border-strong: #A4D5BF;
        --text: #0F172A;
        --text-muted: #6B7280;
        --eyebrow: #94A3B8;
        --primary: #1AA7EC;
        --accent: #2FB47C;
        --accent-strong: #1E9C66;
        --danger: #F87171;
        --shadow-soft: 0 18px 40px rgba(31, 102, 91, 0.16);
    }

    body {
        background: radial-gradient(circle at 10% 10%, rgba(46,196,182,0.18), transparent 45%),
                    radial-gradient(circle at 80% 0%, rgba(16,185,129,0.18), transparent 35%),
                    linear-gradient(180deg, #F7FCFA, #ECF5F1 65%, #E7F3EF);
        color: var(--text);
        font-family: 'Inter', 'SF Pro Display', 'Noto Sans Thai', sans-serif;
    }

    ::selection {
        background: rgba(34, 197, 94, 0.35);
        color: #050505;
    }

    .property-card *::selection {
        background: rgba(34, 197, 94, 0.45);
        color: #050505;
    }

    section.main .block-container {
        max-width: 1320px;
        padding: 0 12px 40px;
    }

    .top-nav {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        border: 1px solid rgba(255,255,255,0.6);
        background: linear-gradient(140deg, rgba(47,180,124,0.18), rgba(26,167,236,0.18));
        border-radius: 26px;
        padding: 18px;
        flex-wrap: wrap;
        gap: 16px;
        box-shadow: var(--shadow-soft);
        position: sticky;
        top: 0;
        z-index: 10;
        backdrop-filter: blur(8px);
    }

    .brand-lockup {
        display: flex;
        flex-direction: column;
        gap: 4px;
    }

    .brand-lockup .eyebrow {
        text-transform: uppercase;
        letter-spacing: 0.16em;
        font-size: 11px;
        color: var(--eyebrow);
    }

    .brand-lockup h1 {
        margin: 0;
        font-size: 28px;
        color: var(--text);
    }

    .brand-lockup span {
        color: var(--text-muted);
        font-size: 14px;
    }

    .nav-actions {
        display: flex;
        gap: 12px;
        align-items: center;
        flex-wrap: wrap;
    }

    .hero-shell {
        margin-top: 20px;
        border-radius: 44px;
        background: linear-gradient(145deg, #EEF2FF, #E6FBF4);
        padding: 30px 32px 32px;
        box-shadow: 0 36px 70px rgba(15,23,42,0.16);
        display: flex;
        flex-direction: column;
        gap: 18px;
        position: relative;
    }

    .hero-shell::after {
        content: "";
        position: absolute;
        inset: 10px;
        border-radius: 38px;
        border: 1px solid rgba(255,255,255,0.65);
        pointer-events: none;
    }

    .hero-topline {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 16px;
    }

    .hero-location-chip {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 10px 18px;
        border-radius: 999px;
        background: rgba(255,255,255,0.92);
        border: 1px solid rgba(15,23,42,0.05);
        box-shadow: 0 12px 24px rgba(15,23,42,0.08);
    }

    .hero-location-icon {
        width: 38px;
        height: 38px;
        border-radius: 50%;
        background: linear-gradient(135deg, #8A7BFF, #6AD7FF);
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 18px;
        color: #fff;
        box-shadow: 0 10px 18px rgba(106,215,255,0.35);
    }

    .hero-location-chip label {
        display: block;
        font-size: 11px;
        letter-spacing: 0.2em;
        text-transform: uppercase;
        color: rgba(15,23,42,0.5);
    }

    .hero-location-chip strong {
        display: block;
        font-size: 18px;
        color: #0F172A;
    }

    .hero-alert-bubble {
        width: 48px;
        height: 48px;
        border-radius: 24px;
        background: rgba(255,255,255,0.95);
        border: 1px solid rgba(15,23,42,0.08);
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 20px;
        box-shadow: 0 14px 28px rgba(15,23,42,0.16);
    }

    .hero-heading h1 {
        margin: 8px 0 4px;
        font-size: clamp(32px, 5vw, 42px);
        line-height: 1.1;
        color: #0F172A;
    }

    .hero-heading p {
        margin: 0;
        font-size: 16px;
        color: rgba(15,23,42,0.6);
    }

    .hero-eyebrow-small {
        font-size: 12px;
        letter-spacing: 0.32em;
        text-transform: uppercase;
        color: rgba(15,23,42,0.55);
    }

    .hero-search-group {
        display: grid;
        grid-template-columns: minmax(0, 1fr) 60px 60px;
        gap: 12px;
        align-items: center;
    }

    .search-pill-shell {
        border-radius: 999px;
        background: rgba(255,255,255,0.95);
        padding: 10px 20px 10px 54px;
        min-height: 64px;
        display: flex;
        align-items: center;
        position: relative;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.9);
    }

    .search-pill-shell::before {
        content: "";
        position: absolute;
        left: 22px;
        width: 22px;
        height: 22px;
        background: url('data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="%230F172A"%3E%3Cpath stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m21 21-4.35-4.35M10 18a8 8 0 1 0 0-16 8 8 0 0 0 0 16z"/%3E%3C/svg%3E') center/contain no-repeat;
        opacity: 0.45;
    }

    .search-pill-shell div[data-baseweb="input"] {
        border: none;
        background: transparent;
        box-shadow: none;
    }

    .search-pill-shell input {
        border: none !important;
        background: transparent !important;
        font-size: 18px;
        font-weight: 600;
        color: #0F172A;
        padding: 0;
    }

    .round-icon-button .stButton > button {
        width: 100%;
        height: 64px;
        border-radius: 999px;
        border: none;
        background: rgba(255,255,255,0.92);
        font-size: 16px;
        font-weight: 600;
        box-shadow: 0 12px 28px rgba(15,23,42,0.12);
        color: #0F172A;
    }

    .chip-toolbar .chip-button.voice .stButton > button {
        background: linear-gradient(135deg, #0F172A, #1F2650);
        color: #fff;
        border: none;
        box-shadow: 0 18px 30px rgba(15,23,42,0.25);
    }

    .category-pills {
        display: flex;
        gap: 12px;
        flex-wrap: nowrap;
        overflow-x: auto;
        padding-bottom: 6px;
        scroll-snap-type: x proximity;
    }

    .category-pills::-webkit-scrollbar {
        height: 0px;
    }

    .category-pills .stButton > button {
        border-radius: 999px;
        border: 1px solid rgba(15,23,42,0.08);
        background: rgba(255,255,255,0.88);
        padding: 12px 20px;
        font-weight: 600;
        color: #0F172A;
        box-shadow: 0 12px 24px rgba(15,23,42,0.1);
    }

    .category-pills .stButton > button[data-testid="baseButton-primary"],
    .category-pills .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #8A7BFF, #6AD7FF);
        color: #fff;
        border: none;
        box-shadow: 0 16px 30px rgba(109,141,255,0.35);
    }

    .control-bar {
        display: flex;
        gap: 12px;
        flex-wrap: wrap;
        margin-top: 6px;
    }

    .control-bar > div {
        flex: 1 1 200px;
        border-radius: 18px;
        border: 1px solid rgba(15,23,42,0.08);
        background: #fff;
        padding: 10px 14px;
        box-shadow: 0 10px 24px rgba(15,23,42,0.08);
    }

    .control-bar .nav-chip,
    .control-bar .heart-control button {
        width: 100%;
    }

    .nav-chip,
    .heart-control button,
    .pill-link,
    .cta-heart {
        border-radius: 999px;
        border: 1px solid var(--border);
        background: #fff;
        color: var(--text);
        font-size: 13px;
        padding: 8px 18px;
        font-weight: 600;
        box-shadow: 0 12px 28px rgba(15,23,42,0.08);
    }

    .heart-control {
        min-width: 120px;
    }

    .heart-control.saved-active button,
    .cta-heart.saved {
        background: var(--accent);
        border-color: var(--accent);
        color: #f0fdf4;
        box-shadow: 0 14px 30px rgba(31,173,116,0.35);
    }

    .metric-row {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap: 18px;
        margin: 20px 0;
    }

    .metric-pill {
        padding: 20px;
        background: var(--panel);
        border-radius: 26px;
        border: 1px solid rgba(255,255,255,0.9);
        box-shadow: var(--shadow-soft);
    }

    .metric-pill h3 {
        margin: 6px 0 0;
        font-size: 32px;
        color: var(--text);
    }

    .metric-pill span {
        color: var(--eyebrow);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.16em;
    }

    .filter-card {
        padding: 24px;
        background: var(--panel);
        border-radius: 28px;
        border: 1px solid rgba(255,255,255,0.9);
        margin-bottom: 22px;
        box-shadow: var(--shadow-soft);
    }

    .active-keyword-pill {
        display: inline-flex;
        justify-content: flex-end;
        width: 100%;
        padding: 10px 16px;
        border-radius: 999px;
        border: 1px dashed var(--border-strong);
        background: var(--surface-alt);
        font-size: 12px;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--eyebrow);
        gap: 6px;
    }

    .active-keyword-pill .pill-icon {
        font-size: 14px;
    }

    .filter-card [data-baseweb="slider"] {
        display: none !important;
    }

    .filter-card label {
        color: var(--eyebrow);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.18em;
    }

    .property-grid-row div[data-testid="stHorizontalBlock"] {
        display: flex;
        flex-direction: column;
        gap: 20px !important;
    }

    .property-grid-row div[data-testid="column"] {
        flex: 1 1 100%;
        min-width: 0;
        display: flex;
    }

    .property-grid-row div[data-testid="column"] > div {
        width: 100%;
        display: flex;
    }

    .property-card {
        border-radius: 32px;
        background: #ffffff;
        padding: 18px;
        box-shadow: 0 20px 40px rgba(15, 23, 42, 0.12);
        display: flex;
        flex-direction: column;
        gap: 16px;
    }

    .card-photo {
        position: relative;
        display: block;
        border-radius: 28px;
        overflow: hidden;
        box-shadow: inset 0 0 0 1px rgba(255,255,255,0.6);
    }

    .card-photo img {
        width: 100%;
        height: 220px;
        object-fit: cover;
        display: block;
        transition: transform 0.25s ease;
    }

    .card-photo:hover img {
        transform: scale(1.03);
    }

    .photo-top-row,
    .photo-bottom-row {
        position: absolute;
        left: 14px;
        right: 14px;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }

    .photo-top-row { top: 14px; }
    .photo-bottom-row { bottom: 14px; }

    .photo-pill {
        padding: 6px 14px;
        border-radius: 999px;
        background: rgba(255,255,255,0.92);
        font-size: 11px;
        letter-spacing: 0.16em;
        text-transform: uppercase;
        color: var(--accent-strong);
        font-weight: 600;
    }

    .photo-actions {
        display: flex;
        gap: 8px;
    }

    .icon-btn {
        width: 38px;
        height: 38px;
        border-radius: 14px;
        border: none;
        background: rgba(255,255,255,0.9);
        box-shadow: 0 10px 24px rgba(15,23,42,0.18);
        display: inline-flex;
        align-items: center;
        justify-content: center;
    }

    .icon-btn::before {
        content: "";
        width: 18px;
        height: 18px;
        display: block;
        background-size: contain;
        background-repeat: no-repeat;
    }

    .icon-btn.share::before {
        background-image: url('data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="%230F172A"%3E%3Cpath stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 8.517a3 3 0 1 0-2-5.517 3 3 0 0 0 2 5.517zM7 14.517a3 3 0 1 0-2 5.517 3 3 0 0 0 2-5.517z"/%3E%3Cpath stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m7.5 13.5 9-5"/%3E%3Cpath stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m7.5 10.5 9 5"/%3E%3C/svg%3E');
    }

    .icon-btn.heart::before {
        background-image: url('data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="%23ec4899"%3E%3Cpath stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4.318 6.318a4.5 4.5 0 0 1 6.364 0L12 7.636l1.318-1.318a4.5 4.5 0 1 1 6.364 6.364L12 21.364l-7.682-7.682a4.5 4.5 0 0 1 0-6.364z"/%3E%3C/svg%3E');
    }

    .icon-btn.heart.saved {
        background: #ec4899;
    }

    .icon-btn.heart.saved::before {
        background-image: url('data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="white"%3E%3Cpath d="M12 21.35l-1.45-1.32C5.4 15.36 2 12.28 2 8.5 2 5.42 4.42 3 7.5 3c1.74 0 3.41.81 4.5 2.09A6 6 0 0 1 21 8.5c0 3.78-3.4 6.86-8.55 11.54L12 21.35z"/%3E%3C/path%3E');
    }

    .icon-btn.heart.disabled {
        opacity: 0.4;
        cursor: not-allowed;
    }

    .sale-badge,
    .meta-pill {
        padding: 6px 14px;
        border-radius: 14px;
        font-size: 12px;
        font-weight: 600;
        color: #0F172A;
        background: rgba(255,255,255,0.92);
    }

    .sale-badge {
        text-transform: uppercase;
        letter-spacing: 0.12em;
    }

    .sale-pill {
        display: inline-flex;
        padding: 6px 14px;
        border-radius: 999px;
        background: rgba(47,180,124,0.12);
        border: 1px solid var(--border-strong);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: var(--accent-strong);
    }

    .card-body {
        display: flex;
        flex-direction: column;
        gap: 12px;
        cursor: pointer;
    }

    .card-body h4 {
        margin: 0;
        font-size: 20px;
        color: #0F172A;
    }

    .card-location {
        margin: 0;
        font-size: 14px;
        color: var(--text-muted);
    }

    .spec-row {
        display: flex;
        justify-content: space-between;
        background: #F6FBF8;
        border-radius: 18px;
        padding: 10px 14px;
        font-size: 13px;
        color: #0F172A;
    }

    .spec-row span {
        display: inline-flex;
        gap: 4px;
        align-items: baseline;
    }

    .agent-chip {
        display: flex;
        align-items: center;
        gap: 12px;
        border: 1px solid rgba(15,23,42,0.08);
        border-radius: 18px;
        padding: 10px 14px;
        background: #FFFFFF;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.8);
    }

    .agent-avatar {
        width: 38px;
        height: 38px;
        border-radius: 14px;
        background: rgba(33,179,122,0.12);
        display: flex;
        align-items: center;
        justify-content: center;
    }

    .agent-chip strong {
        display: block;
        color: #0F172A;
    }

    .agent-chip span {
        font-size: 12px;
        color: rgba(15,23,42,0.6);
    }

    .price-cta-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        border-top: 1px solid rgba(15,23,42,0.08);
        padding-top: 14px;
        gap: 16px;
    }

    .price-stack {
        display: flex;
        flex-direction: column;
        gap: 4px;
    }

    .price-stack span {
        font-size: 12px;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--eyebrow);
    }

    .price-stack strong {
        font-size: 26px;
        color: #0F172A;
    }

    .buy-pill {
        padding: 14px 28px;
        border-radius: 18px;
        border: none;
        background: linear-gradient(120deg, #21B37A, #1AA7EC);
        color: #ffffff;
        font-weight: 600;
        letter-spacing: 0.04em;
        text-decoration: none;
        box-shadow: 0 14px 28px rgba(32,178,123,0.35);
    }

    .type-chip {
        display: inline-flex;
        padding: 6px 14px;
        border-radius: 999px;
        border: 1px solid var(--border);
        color: var(--primary);
        font-size: 12px;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        background: var(--surface-alt);
    }

    .location-line {
        display: flex;
        flex-direction: column;
        align-items: flex-start;
        gap: 6px;
    }

    .location-text {
        display: flex;
        align-items: center;
        gap: 8px;
        flex-wrap: wrap;
    }

    .location-pill {
        padding: 6px 14px;
        border-radius: 999px;
        background: rgba(148, 163, 184, 0.12);
        border: 1px dashed rgba(148, 163, 184, 0.35);
        font-size: 13px;
    }

    .map-action-row {
        display: flex;
        align-items: center;
        gap: 8px;
        flex-wrap: wrap;
        margin-top: 6px;
    }

    .map-action-row.inline {
        min-height: 36px;
    }

    .map-action-row.inline.placeholder .map-placeholder {
        padding: 6px 12px;
        border-radius: 10px;
        background: rgba(148, 163, 184, 0.08);
        border: 1px dashed rgba(148, 163, 184, 0.25);
        font-size: 12px;
        color: var(--text-muted);
    }

    .map-action-label {
        font-size: 11px;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }

    .map-icon {
        width: 34px;
        height: 34px;
        border-radius: 12px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        font-weight: 700;
        color: var(--accent-strong);
        text-decoration: none;
        border: 1px solid var(--border);
        background: var(--surface-alt);
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.6);
    }

    .map-icon span {
        font-size: 11px;
        letter-spacing: 0.08em;
        color: inherit;
    }

    .map-icon.google,
    .map-icon.apple {
        background: var(--panel);
        color: var(--text);
    }

    .detail-location-card {
        border: 1px dashed var(--border);
        border-radius: 16px;
        padding: 14px;
        background: var(--panel);
        margin-top: 12px;
    }

    .price-chip {
        min-width: 130px;
        padding: 8px 12px;
        border-radius: 12px;
        border: 1px solid var(--border);
        background: var(--panel);
        text-align: right;
    }

    .price-chip .primary {
        font-size: 18px;
        font-weight: 700;
        color: var(--accent);
    }

    .price-chip .secondary {
        font-size: 12px;
        color: var(--text-muted);
        letter-spacing: 0.08em;
        text-transform: uppercase;
    }

    .muted {
        color: var(--text-muted);
        font-size: 13px;
    }

    .quick-meta {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(100px, 1fr));
        gap: 8px;
        border: 1px solid var(--border);
        background: var(--panel);
        border-radius: 16px;
        padding: 12px;
    }

    .quick-meta .item {
        background: var(--surface-alt);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 8px 10px;
        color: var(--text);
        box-shadow: 0 8px 16px rgba(17, 24, 39, 0.04);
    }

    .quick-meta .item span {
        display: block;
        font-size: 10px;
        letter-spacing: 0.08em;
        color: var(--text-muted);
        text-transform: uppercase;
    }

    .quick-meta .item strong {
        display: block;
        font-size: 15px;
        color: var(--text);
        margin-top: 4px;
    }

    .compact-meta-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
        gap: 12px;
        margin-top: 12px;
    }

    .compact-meta-grid div {
        background: var(--surface-alt);
        border: 1px solid var(--border);
        border-radius: 18px;
        padding: 12px 14px;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.5);
    }

    .compact-meta-grid span {
        font-size: 11px;
        letter-spacing: 0.14em;
        color: var(--eyebrow);
        text-transform: uppercase;
    }

    .compact-meta-grid strong {
        display: block;
        font-size: 16px;
        color: var(--text);
        margin-top: 6px;
    }

    .room-icon-row {
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
    }

    .room-icon-card {
        flex: 1 1 140px;
        min-width: 130px;
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 8px 12px;
        border-radius: 14px;
        background: var(--panel);
        border: 1px solid var(--border);
        box-shadow: 0 8px 16px rgba(17, 24, 39, 0.06);
    }

    .icon-circle {
        width: 36px;
        height: 36px;
        border-radius: 12px;
        background: var(--surface);
        border: 1px solid var(--border);
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 18px;
        color: var(--text);
    }

    .icon-circle.bath {
        background: rgba(22, 163, 74, 0.12);
        border-color: rgba(22, 163, 74, 0.4);
    }

    .room-icon-card span.label {
        display: block;
        font-size: 10px;
        letter-spacing: 0.08em;
        color: var(--text-muted);
        text-transform: uppercase;
    }

    .room-icon-card strong {
        display: block;
        font-size: 18px;
        color: var(--text);
        margin-top: 4px;
    }

    .card-body {
        flex: 1;
        display: flex;
        flex-direction: column;
        gap: 8px;
    }

    .card-description {
        margin: 0;
        color: var(--text);
        font-size: 13px;
        line-height: 1.4;
        display: -webkit-box;
        -webkit-line-clamp: 3;
        -webkit-box-orient: vertical;
        overflow: hidden;
        min-height: 56px;
    }

    .card-footer {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 14px;
        margin-top: 6px;
    }

    .card-compact {
        display: flex;
        flex-direction: column;
        gap: 10px;
        flex: 1;
        height: 100%;
        justify-content: space-between;
    }

    .card-top-row {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 16px;
    }

    .card-stat-row {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
        gap: 10px;
        margin-top: 6px;
    }

    .stat-chip {
        background: rgba(15,23,42,0.06);
        border-radius: 16px;
        padding: 10px 14px;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.4);
    }

    .stat-chip span {
        display: block;
        font-size: 10px;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--eyebrow);
    }

    .stat-chip strong {
        display: block;
        font-size: 17px;
        color: var(--text);
        margin-top: 4px;
    }

    .card-footer-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 12px;
        flex-wrap: wrap;
        margin-top: auto;
        border-top: 1px solid rgba(15,23,42,0.08);
        padding-top: 12px;
    }

    .card-footer-actions {
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
        align-items: center;
        justify-content: flex-end;
    }

    .sale-pill {
        margin-top: 6px;
        display: inline-flex;
        padding: 8px 18px;
        border-radius: 18px;
        background: rgba(47,180,124,0.12);
        border: none;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.18em;
        color: var(--accent-strong);
    }
    }

    .cta-pill.filled {
        background: linear-gradient(135deg, rgba(59,130,246,0.8), rgba(59,130,246,0.4));
        border-color: rgba(59,130,246,0.8);
        color: #0f172a;
    }

    .card-description.compact {
        min-height: auto;
        -webkit-line-clamp: 2;
        color: var(--text-muted);
    }

    .pricing-hint {
        font-size: 12px;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        font-weight: 600;
    }

    .pricing-hint.good {color: var(--accent);}
    .pricing-hint.fair {color: #fbbf24;}
    .pricing-hint.bad {color: var(--danger);}

    .detail-info-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
        gap: 10px;
        margin: 12px 0 8px 0;
    }

    .detail-page-shell {
        margin: 12px 0;
    }

    .cta-heart {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        text-decoration: none;
    }

    .price-chip {
        min-width: 140px;
        padding: 12px 16px;
        border-radius: 18px;
        border: none;
        background: #0c2a1f;
        text-align: right;
        color: #f8fafc;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.18);
    }

    .price-chip .primary {
        font-size: 22px;
        font-weight: 700;
        color: #ffffff;
    }

    .price-chip .secondary {
        font-size: 11px;
        color: rgba(255,255,255,0.68);
        letter-spacing: 0.18em;
        text-transform: uppercase;
    }
    .detail-hero-overlay h2 {
        margin: 4px 0;
        font-size: 28px;
        color: #ffffff;
    }

    .detail-hero-overlay p {
        margin: 0;
        color: #e2e8f0;
    }

    .detail-hero-actions {
        display: flex;
        flex-direction: column;
        gap: 10px;
    }

    .detail-info-grid .detail-stat {
        background: var(--panel);
        border-radius: 12px;
        border: 1px solid var(--border);
        padding: 10px 12px;
    }

    .detail-info-grid .detail-stat span {
        display: block;
        font-size: 11px;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }

    .detail-info-grid .detail-stat strong {
        display: block;
        margin-top: 4px;
        font-size: 16px;
        color: var(--text);
    }

    .detail-body {
        background: var(--panel);
        border-radius: 14px;
        border: 1px solid var(--border);
        padding: 14px;
        margin-top: 8px;
    }

    .detail-body h4 {
        margin: 0 0 6px 0;
        font-size: 16px;
    }

    .detail-body p {
        margin: 0 0 8px 0;
        color: #e2e8f0;
        line-height: 1.4;
        font-size: 14px;
    }

    .detail-body .detail-duo {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 12px;
        margin-top: 10px;
    }

    .detail-links {
        display: flex;
        justify-content: space-between;
        gap: 10px;
        flex-wrap: wrap;
        margin-top: 12px;
    }

    .price-chip {
        min-width: 150px;
        padding: 10px 16px;
        border-radius: 20px;
        border: 1px solid var(--border);
        background: var(--surface-alt);
        text-align: right;
    }

    .price-chip .primary {
        font-size: 22px;
        font-weight: 700;
        color: var(--text);
    }

    .price-chip .secondary {
        font-size: 11px;
        color: var(--eyebrow);
        letter-spacing: 0.18em;
        text-transform: uppercase;
    }
        color: var(--text);
        text-decoration: none;
        font-weight: 600;
        font-size: 13px;
    }

    .cta-heart.saved {
        background: var(--accent);
        color: #ffffff;
        border-color: var(--accent);
    }

    .price-bar-wrapper {
        margin-top: 14px;
        min-height: 78px;
        display: flex;
        flex-direction: column;
        justify-content: center;
    }

    .price-bar-wrapper.spacer {
        justify-content: flex-end;
    }

    .price-bar-track {
        position: relative;
        height: 10px;
        border-radius: 999px;
        background: rgba(15,23,42,0.08);
        box-shadow: inset 0 1px 2px rgba(17, 24, 39, 0.04);
    }

    .price-bar-indicator {
        position: absolute;
        top: -4px;
        width: 3px;
        height: 18px;
        border-radius: 2px;
        background: var(--accent);
        box-shadow: 0 0 0 5px rgba(47, 180, 124, 0.25);
    }

    .price-bar-labels {
        display: flex;
        justify-content: space-between;
        font-size: 10px;
        color: var(--eyebrow);
        margin-top: 4px;
        letter-spacing: 0.12em;
    }

    .price-compare {
        margin-top: 6px;
        font-size: 13px;
        font-weight: 600;
        color: var(--text-muted);
    }

    .price-compare.good {color: var(--accent);}
    .price-compare.fair {color: var(--text-muted);}
    .price-compare.bad {color: var(--danger);}

    .value-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 14px;
        margin-top: 16px;
    }

    .value-chip {
        background: #0f2c21;
        border-radius: 22px;
        padding: 16px;
        display: flex;
        flex-direction: column;
        gap: 8px;
        color: #e0fbea;
    }

    .value-chip-header {
        display: flex;
        align-items: center;
        gap: 6px;
    }

    .value-chip .value-label {
        font-size: 11px;
        letter-spacing: 0.18em;
        text-transform: uppercase;
        color: rgba(255,255,255,0.65);
    }

    .value-chip .value-icon {
        font-size: 16px;
        line-height: 1;
    }

    .value-chip strong {
        font-size: 22px;
        color: #5ef1b1;
    }

    .thumb-row {
        display: flex;
        gap: 8px;
        flex-wrap: wrap;
        margin: 12px 0;
    }

    .thumb-link {
        width: 110px;
        height: 70px;
        border-radius: 12px;
        overflow: hidden;
        border: 2px solid transparent;
        display: block;
    }

    .thumb-link img {
        width: 100%;
        height: 100%;
        object-fit: cover;
        display: block;
    }


    .modal-shell {
        background: rgba(5, 10, 23, 0.9);
        border-radius: 20px;
        border: 1px solid rgba(148, 163, 184, 0.2);
        padding: 14px;
        box-shadow: 0 20px 45px rgba(2, 6, 23, 0.85);
        margin-bottom: 14px;
    }

    .modal-photo-wrapper {
        position: relative;
        border-radius: 16px;
        overflow: hidden;
        border: 1px solid rgba(148, 163, 184, 0.25);
        background: #030712;
    }

    .modal-photo-wrapper img {
        width: 100%;
        height: 520px;
        object-fit: cover;
        display: block;
    }

    .modal-photo-overlay {
        position: absolute;
        inset: 0;
        padding: 18px;
        display: flex;
        justify-content: space-between;
        align-items: flex-end;
        background: linear-gradient(180deg, rgba(2,6,23,0.0) 0%, rgba(2,6,23,0.85) 100%);
        color: #ffffff;
        pointer-events: none;
    }

    .modal-photo-overlay h3 {
        margin: 6px 0 2px 0;
        font-size: 22px;
    }

    .modal-photo-overlay p {
        margin: 0;
        color: var(--text-muted);
        font-size: 14px;
    }

    .modal-pill {
        display: inline-flex;
        padding: 4px 12px;
        border-radius: 999px;
        border: 1px solid rgba(148, 163, 184, 0.35);
        text-transform: uppercase;
        letter-spacing: 0.08em;
        font-size: 11px;
        background: rgba(17, 24, 39, 0.65);
    }

    .modal-hint {
        font-size: 12px;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: rgba(255,255,255,0.75);
        text-align: right;
    }

    .modal-nav-grid {
        margin: 6px 0 4px 0;
    }

    .modal-nav-grid div[data-testid="stButton"] button {
        width: 100%;
        border-radius: 999px;
        border: 1px solid rgba(148, 163, 184, 0.35);
        background: rgba(17, 24, 39, 0.65);
        color: #e2e8f0;
        font-weight: 600;
    }

    .modal-nav-grid div[data-testid="stButton"] button:disabled {
        opacity: 0.4;
    }

    .modal-chip {
        text-align: center;
        padding: 8px 0;
        letter-spacing: 0.08em;
    }

    .modal-thumb-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(110px, 1fr));
        gap: 10px;
        margin: 12px 0 6px 0;
    }

    .modal-thumb-grid a {
        border-radius: 12px;
        overflow: hidden;
        border: 2px solid transparent;
        display: block;
        height: 70px;
    }

    .modal-thumb-grid a img {
        width: 100%;
        height: 100%;
        object-fit: cover;
        display: block;
    }

    .modal-thumb-grid a.active {
        border-color: var(--primary);
    }

    .modal-close-row {
        display: flex;
        justify-content: flex-end;
        margin-top: 4px;
    }

    .modal-close-row div[data-testid="stButton"] button {
        border-radius: 999px;
        background: rgba(239, 68, 68, 0.18);
        border: 1px solid rgba(239, 68, 68, 0.35);
        color: #fecaca;
    }

    .original-pill {
        font-size: 11px;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }

    .original-note {
        font-size: 12px;
        color: var(--text-muted);
        margin-top: 4px;
    }

    .login-hint {
        font-size: 13px;
        color: var(--text-muted);
    }

    .property-grid-row div[data-testid="column"] > div {
        height: 100%;
    }

    @media (min-width: 900px) {
        .property-grid-row div[data-testid="column"] {
            flex: 1 1 calc(50% - 20px);
        }
    }

    @media (max-width: 1100px) {
        .property-grid-row div[data-testid="column"] {
            flex: 1 1 calc(50% - 22px);
        }

        .photo-link img {
            height: 200px;
        }
    }

    @media (max-width: 768px) {
        .top-nav {
            flex-direction: column;
            align-items: flex-start;
        }

        .card-cta-row {
            flex-direction: column;
            align-items: flex-start;
        }

        .price-stack {
            width: 100%;
        }

        .buy-pill {
            width: 100%;
            justify-content: center;
        }

        .property-grid-row div[data-testid="column"] {
            flex: 1 1 100%;
        }

        .photo-link img {
            height: 180px;
        }
    }

    .bottom-app-nav {
        position: sticky;
        bottom: 12px;
        width: 100%;
        margin-top: 24px;
    }

    .bottom-app-nav .nav-shell {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 10px;
        padding: 10px 22px;
        border-radius: 999px;
        border: 1px solid rgba(255,255,255,0.9);
        background: var(--panel);
        box-shadow: var(--shadow-soft);
    }

    .bottom-app-nav div[data-testid="stButton"] > button {
        background: transparent;
        border: none;
        font-weight: 600;
        color: var(--text-muted);
        padding: 8px 0;
    }

    .bottom-app-nav div[data-testid="stButton"] > button[kind="primary"],
    .bottom-app-nav div[data-testid="stButton"] > button[data-testid="baseButton-primary"] {
        color: var(--accent-strong);
    }
    </style>
    """, unsafe_allow_html=True)

# --- DB HELPER ---

def run_query(query, params=()):
    try:
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        cur.execute(query, params)
        if query.strip().upper().startswith("SELECT"):
            col_names = [desc[0] for desc in cur.description]
            data = cur.fetchall()
            conn.commit()
            cur.close()
            conn.close()
            return pd.DataFrame(data, columns=col_names)
        else:
            conn.commit()
            cur.close()
            conn.close()
            return None
    except Exception as e:
        st.error(f"Database Query Failed: {e}")
        return None


@st.cache_data(ttl=600, show_spinner=False)
def load_properties_df():
    """Cached wrapper to keep the UI responsive between reruns."""
    return run_query("SELECT * FROM properties")


def sanitize_rich_text(value):
    if value is None:
        return ""
    text = html.unescape(str(value))
    text = TAG_RE.sub(" ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_text_field(value, default=""):
    cleaned = sanitize_rich_text(value)
    return cleaned if cleaned else default


def compact_html(html_str):
    if not html_str:
        return ""
    lines = [line.strip() for line in str(html_str).splitlines() if line.strip()]
    return "".join(lines)


def prioritize_photos(urls):
    ordered = []
    seen = set()
    for url in urls:
        if not url:
            continue
        url = url.strip()
        if not url.lower().startswith("http"):
            continue
        if url not in seen:
            seen.add(url)
            ordered.append(url)

    def score(url_with_index):
        idx, url = url_with_index
        penalty = idx
        if is_map_like(url):
            penalty += 200
        if looks_like_property_photo(url):
            penalty -= 50
        return penalty

    indexed = list(enumerate(ordered))
    prioritized = [url for _, url in sorted(indexed, key=score)]
    return prioritized


def is_map_like(url):
    if not url:
        return False
    lowered = url.lower()
    return any(keyword in lowered for keyword in PHOTO_AVOID_KEYWORDS)


def looks_like_property_photo(url):
    if not url:
        return False
    lowered = url.lower()
    if any(keyword in lowered for keyword in PHOTO_FAVOR_KEYWORDS):
        return True
    if re.search(r"/asset[-_/]", lowered):
        return True
    return False


def normalize_area_label(value):
    if not value:
        return None
    text = clean_text_field(value)
    text = text.lower().strip()
    if not text:
        return None
    return text


def build_map_links(lat, lon, fallback_location):
    query = None
    try:
        if lat is not None and lon is not None:
            lat_val = float(lat)
            lon_val = float(lon)
            query = f"{lat_val:.6f},{lon_val:.6f}"
    except (TypeError, ValueError):
        query = None

    if not query:
        cleaned = clean_text_field(fallback_location)
        if cleaned:
            query = cleaned

    if not query:
        return {}

    encoded = quote_plus(query)
    return {
        "google": f"https://www.google.com/maps/search/?api=1&query={encoded}",
        "apple": f"https://maps.apple.com/?q={encoded}",
    }


def build_pagination_sequence(current_page, total_pages, window=1):
    if total_pages <= 7:
        return list(range(1, total_pages + 1))
    sequence = [1]
    left = max(2, current_page - window)
    right = min(total_pages - 1, current_page + window)
    if left > 2:
        sequence.append('...')
    else:
        left = 2
    for page_num in range(left, right + 1):
        sequence.append(page_num)
    if right < total_pages - 1:
        sequence.append('...')
    sequence.append(total_pages)
    return sequence


def extract_primary_photo(photo_field):
    if not photo_field:
        return DEFAULT_PLACEHOLDER_IMAGE
    photo_text = str(photo_field)
    candidates = re.split(r"[,|]", photo_text)
    html_matches = re.findall(r"src=[\"'](http[^\"'>]+)", photo_text)
    loose_matches = re.findall(r"(https?://[^\s\"'<>]+)", photo_text)
    prioritized = prioritize_photos(candidates + html_matches + loose_matches)
    if not prioritized:
        return DEFAULT_PLACEHOLDER_IMAGE
    for url in prioritized:
        if looks_like_property_photo(url):
            return url
    for url in prioritized:
        if not is_map_like(url):
            return url
    return prioritized[0]


def extract_all_photos(photo_field):
    if not photo_field:
        return [DEFAULT_PLACEHOLDER_IMAGE]
    photo_text = str(photo_field)
    raw_candidates = re.split(r"[,|]", photo_text)
    html_matches = re.findall(r"src=[\"'](http[^\"'>]+)", photo_text)
    loose_matches = re.findall(r"(https?://[^\s\"'<>]+)", photo_text)
    prioritized = prioritize_photos(raw_candidates + html_matches + loose_matches)
    if not prioritized:
        return [DEFAULT_PLACEHOLDER_IMAGE]
    property_first = [url for url in prioritized if looks_like_property_photo(url)]
    remainder = [url for url in prioritized if url not in property_first]
    prioritized = property_first + remainder
    primary = property_first[0] if property_first else None
    if primary and prioritized[0] != primary:
        prioritized = [primary] + [url for url in prioritized if url != primary]
    return prioritized


def normalize_property_type(raw_type, fallback_text=""):
    candidate = (raw_type or "").strip()
    haystack = f"{candidate} {fallback_text or ''}".lower()
    for label, keywords in PROPERTY_KEYWORDS.items():
        if any(keyword in haystack for keyword in keywords):
            return label
    return candidate.title() if candidate else "Property"


def ensure_saved_table():
    run_query(
        """
        CREATE TABLE IF NOT EXISTS saved_properties (
            id SERIAL PRIMARY KEY,
            username TEXT NOT NULL,
            property_id INTEGER NOT NULL,
            saved_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(username, property_id)
        )
        """
    )


def ensure_token_expiry_column():
    run_query("ALTER TABLE users ADD COLUMN IF NOT EXISTS remember_token_expires TIMESTAMPTZ")


def get_saved_property_ids(current_user):
    if not current_user:
        return set()
    df = run_query("SELECT property_id FROM saved_properties WHERE username=%s", (current_user,))
    if df is None or df.empty:
        return set()
    cleaned = df['property_id'].dropna().tolist()
    result = set()
    for value in cleaned:
        try:
            result.add(int(value))
        except (TypeError, ValueError):
            continue
    return result


def save_property(current_user, property_id):
    if not current_user or property_id is None:
        return
    run_query(
        """
        INSERT INTO saved_properties (username, property_id, saved_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (username, property_id) DO NOTHING
        """,
        (current_user, property_id)
    )


def remove_saved_property(current_user, property_id):
    if not current_user or property_id is None:
        return
    run_query("DELETE FROM saved_properties WHERE username=%s AND property_id=%s", (current_user, property_id))


def fetch_saved_properties(current_user):
    if not current_user:
        return pd.DataFrame()
    return run_query(
        """
        SELECT p.*
        FROM properties p
        INNER JOIN saved_properties s ON p.id = s.property_id
        WHERE s.username=%s
        ORDER BY s.saved_at DESC
        """,
        (current_user,)
    )


def format_price(value):
    try:
        if value is None:
            return "Contact for price"
        value = float(value)
        if math.isnan(value) or value <= 0:
            return "Contact for price"
        return f"{value:,.0f} THB"
    except (TypeError, ValueError):
        return "Contact for price"


def display_sale_channel(channel):
    mapping = {
        "standard": "Foreclosure Property",
        "direct_sale": "Foreclosure Property",
        "auction": "Auction",
        "short_sale": "Short Sale",
        "bulk": "Bulk Deal"
    }
    return mapping.get(channel, channel.title() if isinstance(channel, str) else "Sale")


def get_query_params():
    params = {}
    try:
        for key, value in st.query_params.items():
            if isinstance(value, (list, tuple)):
                params[key] = value[-1]
            else:
                params[key] = value
    except Exception:
        pass
    return params


def build_query_string(**updates):
    params = get_query_params()
    for key, value in updates.items():
        if value is None:
            params.pop(key, None)
        else:
            params[key] = value
    if not params:
        return ""
    return f"?{urlencode(params)}"


def clear_query_keys(*keys):
    try:
        for key in keys:
            if key in st.query_params:
                del st.query_params[key]
    except Exception:
        pass


ensure_saved_table()
ensure_token_expiry_column()


def set_remember_token(username):
    token = uuid.uuid4().hex
    expires_at = datetime.now(timezone.utc) + timedelta(days=TOKEN_TTL_DAYS)
    run_query("UPDATE users SET remember_token=%s, remember_token_expires=%s WHERE username=%s", (token, expires_at, username))
    try:
        st.query_params["session"] = token
    except Exception:
        pass
    st.session_state['remember_token'] = token
    st.session_state['remember_token_expiry'] = expires_at
    return token


def clear_remember_token(username=None):
    try:
        st.query_params.clear()
    except Exception:
        pass
    st.session_state.pop('remember_token', None)
    st.session_state.pop('remember_token_expiry', None)
    if username:
        run_query("UPDATE users SET remember_token=NULL, remember_token_expires=NULL WHERE username=%s", (username,))


# --- AUTHENTICATOR SETUP ---
def get_users():
    df = run_query("SELECT username, password, role, is_active FROM users")
    users = {}
    if df is not None and not df.empty:
        for _, row in df.iterrows():
            if row['is_active']:
                users[row['username']] = {
                    'name': row['username'],
                    'password': row['password'],
                    'role': row['role']
                }
    return users


# Prepare credentials map (username -> record)
users = get_users()
if "auth_status" not in st.session_state:
    st.session_state.auth_status = None
    st.session_state.username = None
    st.session_state.name = None
    st.session_state.role = None


def refresh_users():
    global users
    users = get_users()


def authenticate_user(email, password):
    refresh_users()
    user = users.get(email)
    if not user:
        return None
    stored_password = user.get('password')
    if stored_password and stored_password == password:
        return user
    return None


def hydrate_session_from_token():
    if st.session_state.auth_status:
        return
    try:
        params = dict(st.query_params)
    except Exception:
        params = {}
    token_list = params.get("session")
    if not token_list:
        return
    token = token_list[0] if isinstance(token_list, list) else token_list
    if not token:
        return
    df = run_query("SELECT username, role, remember_token_expires FROM users WHERE remember_token=%s AND is_active=TRUE", (token,))
    if df is not None and not df.empty:
        row = df.iloc[0]
        expiry = row.get('remember_token_expires')
        expiry_dt = None
        if expiry is not None and not pd.isna(expiry):
            if hasattr(expiry, 'to_pydatetime'):
                expiry_dt = expiry.to_pydatetime()
            else:
                expiry_dt = expiry
        now = datetime.now(timezone.utc)
        if expiry_dt and expiry_dt < now:
            clear_remember_token(row.get('username'))
            return
        st.session_state.auth_status = True
        st.session_state.username = row.get('username')
        st.session_state.name = row.get('username')
        st.session_state.role = row.get('role', 'client')
        st.session_state['remember_token'] = token
        if expiry_dt:
            st.session_state['remember_token_expiry'] = expiry_dt
            buffer_delta = timedelta(days=TOKEN_ROTATE_BUFFER_DAYS)
            if expiry_dt - now <= buffer_delta:
                set_remember_token(row.get('username'))


hydrate_session_from_token()

authentication_status = st.session_state.auth_status
username = st.session_state.username


def logout_user():
    current_user = st.session_state.get('username')
    clear_remember_token(current_user)
    st.session_state.auth_status = None
    st.session_state.username = None
    st.session_state.name = None
    st.session_state.role = None
    st.session_state.pop('login_error', None)
    st.session_state.pop('pending_email', None)
    st.rerun()

def login_screen():
    st.title("🔒 Sniper Bot Terminal")
    if authentication_status is False:
        st.error("Access Denied.")
        st.session_state.auth_status = None
    if st.session_state.auth_status is None:
        st.info("Please enter your email and password.")
        st.caption("Default admin credentials → email: **admin**, password: **admin123**")
        with st.form("login_form"):
            email = st.text_input("Email", value=st.session_state.get('pending_email', ''), placeholder="you@example.com")
            password = st.text_input("Password", type="password")
            remember = st.checkbox("Remember me", value=True)
            submitted = st.form_submit_button("Login", width="stretch")
        if submitted:
            st.session_state.pending_email = email
            user = authenticate_user(email, password)
            if user:
                st.session_state.auth_status = True
                st.session_state.username = email
                st.session_state.name = user.get('name', email)
                st.session_state.role = user.get('role', 'client')
                st.session_state.remember_me = remember
                st.session_state.pop('login_error', None)
                if remember:
                    set_remember_token(email)
                else:
                    clear_remember_token(email)
                st.success("Welcome back! Redirecting...")
                st.rerun()
            else:
                st.session_state.auth_status = False
                st.session_state.login_error = "Invalid email or password."
                st.error(st.session_state.login_error)


def render_inline_login_controls(inline=False):
    if st.session_state.auth_status:
        return

    def login_body():
        st.caption("Default admin credentials → **admin / admin123**")
        login_error = st.session_state.get('login_error')
        if login_error:
            st.error(login_error)
        with st.form("inline_login_form"):
            email = st.text_input("Email", value=st.session_state.get('pending_email', ''), placeholder="you@example.com", key="inline_email")
            password = st.text_input("Password", type="password", key="inline_password")
            remember = st.checkbox("Remember me", value=True, key="inline_remember")
            submitted = st.form_submit_button("Login")
        if submitted:
            st.session_state.pending_email = email
            user = authenticate_user(email, password)
            if user:
                st.session_state.auth_status = True
                st.session_state.username = email
                st.session_state.name = user.get('name', email)
                st.session_state.role = user.get('role', 'client')
                st.session_state.remember_me = remember
                st.session_state.pop('login_error', None)
                if remember:
                    set_remember_token(email)
                else:
                    clear_remember_token(email)
                st.success("Welcome back!")
                st.rerun()
            else:
                st.session_state.auth_status = False
                st.session_state.login_error = "Invalid email or password."
                st.error(st.session_state.login_error)

    if inline:
        login_body()
    else:
        st.markdown("<div class='inline-login-card'>", unsafe_allow_html=True)
        login_body()
        st.markdown("</div>", unsafe_allow_html=True)

# --- DASHBOARD ---
def main_dashboard():
    refresh_users()
    # Translation cache and helper
    translation_cache = {}

    translation_health = {
        "gcp": True,
        "api": True,
        "last_error": ""
    }
    st.session_state['translation_health'] = translation_health

    def translate_text(text, target_lang):
        nonlocal translation_health
        if not text:
            return text
        key = (text, target_lang)
        if key in translation_cache:
            return translation_cache[key]
        def mark_translation_issue(channel, message):
            translation_health[channel] = False
            translation_health['last_error'] = message
        # Prefer GCP Translate when configured
        project = os.getenv('GOOGLE_CLOUD_PROJECT')
        if project and target_lang != 'th':
            try:
                client = translate_client.TranslationServiceClient()
                parent = f"projects/{project}/locations/global"
                response = client.translate_text(
                    request={
                        "parent": parent,
                        "contents": [text],
                        "mime_type": "text/plain",
                        "target_language_code": target_lang,
                    }
                )
                if response and response.translations:
                    translated = clean_text_field(response.translations[0].translated_text, text)
                    translation_cache[key] = translated
                    translation_health['gcp'] = True
                    return translated
            except Exception as exc:
                mark_translation_issue('gcp', f"GCP Translate error: {exc}")
        if GOOGLE_TRANSLATE_API_KEY and target_lang != 'th':
            try:
                url = "https://translation.googleapis.com/language/translate/v2"
                payload = {
                    "q": text,
                    "target": target_lang,
                    "format": "text",
                    "key": GOOGLE_TRANSLATE_API_KEY,
                }
                response = requests.post(url, data=payload, timeout=8)
                if response.status_code == 200:
                    data = response.json()
                    translated = clean_text_field(data['data']['translations'][0]['translatedText'], text)
                    translation_cache[key] = translated
                    translation_health['api'] = True
                    return translated
                mark_translation_issue('api', f"Translate API error: {response.text}")
            except Exception as exc:
                mark_translation_issue('api', f"Translate API exception: {exc}")
        # Fallback public endpoint
        try:
            url = "https://translate.googleapis.com/translate_a/single"
            params = {
                "client": "gtx",
                "sl": "auto",
                "tl": target_lang,
                "dt": "t",
                "q": text
            }
            response = requests.get(url, params=params, timeout=5)
            if response.status_code == 200:
                result = response.json()
                translated = clean_text_field(result[0][0][0], text)
                translation_cache[key] = translated
                return translated
        except Exception:
            pass
        translation_cache[key] = text
        return text

    def format_number(value, unit):
        try:
            val = float(value)
            if math.isnan(val) or val <= 0:
                return '—'
            if abs(val - round(val)) < 0.01:
                val_str = f"{int(round(val)):,}"
            else:
                val_str = f"{val:,.1f}"
            return f"{val_str} {unit}"
        except (TypeError, ValueError):
            return '—'

    def first_valid_measurement(candidates):
        for unit, raw in candidates:
            display = format_number(raw, unit)
            if display != '—':
                return display
        return '—'

    def format_count(value):
        try:
            if value is None:
                return None
            val = float(value)
            if math.isnan(val) or val <= 0:
                return None
            if abs(val - round(val)) < 0.01:
                return str(int(round(val)))
            return f"{val:.1f}"
        except (TypeError, ValueError):
            return None

    # UI labels
    labels = {
            "English": {
                "market": "Market Opportunities",
                "min_price": "Min Price (THB)",
                "max_price": "Max Price (THB)",
                "property_type": "Property Type",
                "rooms": "Rooms",
                "bathrooms": "Bathrooms",
                "listings_found": "Listings Found",
                "page": "Page",
                "price": "Price",
                "location": "Location",
                "description": "Description",
                "contact": "Contact",
                "living_rating": "Living Condition Rating",
                "rent_estimate": "Rent Estimate",
                "investment_rating": "Investment Rating",
                "bank": "Bank",
                "beds": "Bedrooms",
                "baths": "Bathrooms",
                "land_size": "Land Size",
                "size_label": "Living Area"
            },
            "ไทย": {
                "market": "โอกาสในตลาด",
                "min_price": "ราคาขั้นต่ำ (บาท)",
                "max_price": "ราคาสูงสุด (บาท)",
                "property_type": "ประเภทอสังหาริมทรัพย์",
                "rooms": "จำนวนห้อง",
                "bathrooms": "จำนวนห้องน้ำ",
                "listings_found": "รายการที่พบ",
                "page": "หน้า",
                "price": "ราคา",
                "location": "ที่ตั้ง",
                "description": "รายละเอียด",
                "contact": "ติดต่อ",
                "living_rating": "คะแนนสภาพความเป็นอยู่",
                "rent_estimate": "ประมาณค่าเช่า",
                "investment_rating": "คะแนนการลงทุน",
                "bank": "ธนาคาร",
                "beds": "ห้องนอน",
                "baths": "ห้องน้ำ",
                "land_size": "พื้นที่ที่ดิน",
                "size_label": "พื้นที่ใช้สอย"
            }
    }

    lang = st.session_state.get("lang_select", "English")
    show_original = st.session_state.get("show_original", False)
    is_admin = authentication_status and username and users.get(username, {}).get('role') == 'admin'

    if "view_mode" not in st.session_state:
        st.session_state['view_mode'] = 'all'

    if 'property_type_choice' not in st.session_state:
        st.session_state['property_type_choice'] = "All"

    if 'hero_category_active' not in st.session_state:
        st.session_state['hero_category_active'] = "All"

    if 'bottom_nav_active' not in st.session_state:
        st.session_state['bottom_nav_active'] = 'Home'

    if 'focus_section' not in st.session_state:
        st.session_state['focus_section'] = None

    if 'account_panel_open' not in st.session_state:
        st.session_state['account_panel_open'] = False
    if 'notify_new_listings' not in st.session_state:
        st.session_state['notify_new_listings'] = True

    if 'notify_price_drop' not in st.session_state:
        st.session_state['notify_price_drop'] = True

    if 'dark_mode_pref' not in st.session_state:
        st.session_state['dark_mode_pref'] = False

    if 'auto_translate_pref' not in st.session_state:
        st.session_state['auto_translate_pref'] = True

    params = get_query_params()
    save_param = params.get('save')
    save_op = params.get('save_op', 'add')
    if save_param and username:
        try:
            save_id = int(save_param)
            if save_op == 'remove':
                remove_saved_property(username, save_id)
            else:
                save_property(username, save_id)
            st.session_state.pop('saved_ids', None)
            st.session_state.pop('saved_ids_owner', None)
        except (TypeError, ValueError):
            pass
        clear_query_keys('save', 'save_op')
        st.rerun()

    def render_admin_sidebar():
        if not is_admin:
            return
        with st.sidebar:
            st.title("👑 Admin Deck")
            st.info(f"Signed in as {username}")
            show_admin = st.checkbox("Show Admin Panel", value=False, key="admin_panel_toggle")
            if show_admin:
                with st.expander("User Management", expanded=False):
                    new_u = st.text_input("New User", key="admin_new_user")
                    new_p = st.text_input("New Pass", key="admin_new_pass")
                    if st.button("Create Client", key="admin_create_client"):
                        run_query("INSERT INTO users (username, password, role) VALUES (%s, %s, 'client')", (new_u, new_p))
                        refresh_users()
                        st.success("User Created!")
                if st.button("🔄 Force Rescan Now", key="admin_rescan"):
                    st.toast("Triggering Sniper Engine...")
                    os.system("python sniper_engine.py")
                    st.success("Scan Complete. Refreshing...")
                    st.rerun()

    with st.sidebar:
        if st.button("↻ Refresh listings", key="refresh_cache_btn"):
            load_properties_df.clear()
            st.toast("Reloading latest feed…")
            st.rerun()

    render_admin_sidebar()

    def load_saved_ids():
        if not authentication_status or not username:
            return set()
        owner = st.session_state.get('saved_ids_owner')
        if owner != username or 'saved_ids' not in st.session_state:
            st.session_state['saved_ids'] = get_saved_property_ids(username)
            st.session_state['saved_ids_owner'] = username
        return st.session_state.get('saved_ids', set())

    saved_ids = load_saved_ids()
    saved_count = len(saved_ids)

    l = labels.get(lang, labels["English"])
    greeting_hour = datetime.now().hour
    if greeting_hour < 12:
        greeting_text = "Good Morning"
    elif greeting_hour < 18:
        greeting_text = "Good Afternoon"
    else:
        greeting_text = "Good Evening"
    hero_name = username.title() if username else "Guest Explorer"
    hero_initials = (username[:2] if username else "TG").upper()

    df = load_properties_df()
    if df is None or df.empty:
        property_options = []
        sale_options = []
        avg_price_by_area = {}
        data_price_min = 0
        data_price_max = 0
    else:
        df = df.copy()
        if 'last_updated' in df.columns:
            df = df.sort_values('last_updated', ascending=False)
        df['property_type'] = df.apply(lambda row: normalize_property_type(row.get('property_type'), row.get('title')), axis=1)
        df['area_key'] = df.get('location').apply(normalize_area_label) if 'location' in df.columns else None
        df['price_numeric'] = pd.to_numeric(df.get('price'), errors='coerce') if 'price' in df.columns else None
        if 'area_key' in df.columns and 'price_numeric' in df.columns:
            area_price_df = df[(df['area_key'].notna()) & (df['price_numeric'] > 0)]
            avg_price_by_area = area_price_df.groupby('area_key')['price_numeric'].mean().to_dict()
        else:
            avg_price_by_area = {}

        property_options = sorted(df['property_type'].dropna().unique()) if 'property_type' in df.columns else []
        sale_options = sorted(df['sale_channel'].dropna().unique()) if 'sale_channel' in df.columns else []
        price_series = df['price'].dropna() if 'price' in df.columns else pd.Series(dtype=float)
        positive_prices = price_series[price_series > 0] if not price_series.empty else pd.Series(dtype=float)
        data_price_min = int(positive_prices.min()) if not positive_prices.empty else 0
        data_price_max = int(positive_prices.max()) if not positive_prices.empty else 0
        if data_price_min == data_price_max:
            data_price_max = data_price_min + 1000000

    st.markdown("<div id='top-anchor'></div>", unsafe_allow_html=True)

    hero_categories = [
        {"label": "House", "icon": "🏠", "aliases": ["Single House", "House", "Townhouse", "Townhome"]},
        {"label": "Villa", "icon": "🏡", "aliases": ["Villa", "Single House", "House"]},
        {"label": "Apart.", "icon": "🏢", "aliases": ["Condo", "Apartment"]},
        {"label": "Hotel", "icon": "🏨", "aliases": ["Hotel", "Commercial"]},
    ]

    def derive_focus_location(dataframe):
        if isinstance(dataframe, pd.DataFrame) and not dataframe.empty and 'area_key' in dataframe.columns:
            area_series = dataframe['area_key'].dropna()
            if not area_series.empty:
                try:
                    return str(area_series.mode().iloc[0])
                except Exception:
                    return str(area_series.iloc[0])
        return "Thailand"

    def determine_active_category(selected_value):
        if not selected_value or selected_value == "All":
            return "All"
        for cat in hero_categories:
            if selected_value in cat.get('aliases', []):
                return cat['label']
        return "All"

    def resolve_category_value(cat_entry):
        aliases = cat_entry.get('aliases', [])
        for alias in aliases:
            if alias in property_options:
                return alias
        return None

    current_choice = st.session_state.get('property_type_choice', "All")
    hero_focus_location = st.session_state.get('hero_focus_location') or derive_focus_location(df)
    st.session_state['hero_focus_location'] = hero_focus_location
    st.session_state['hero_category_active'] = determine_active_category(current_choice)

    st.markdown("<div class='hero-shell'>", unsafe_allow_html=True)

    hero_html = compact_html(
        f"""
        <div class='hero-topline'>
            <div class='hero-location-chip'>
                <div class='hero-location-icon'>📍</div>
                <div>
                    <label>Focus area</label>
                    <strong>{hero_focus_location}</strong>
                </div>
            </div>
            <div class='hero-alert-bubble'>🔔</div>
        </div>
        <div class='hero-heading'>
            <div class='hero-eyebrow-small'>{greeting_text}</div>
            <h1>Search your house by Jesp</h1>
            <p>Hand-picked deals for {hero_name} across {hero_focus_location}.</p>
        </div>
        """
    )
    st.markdown(hero_html, unsafe_allow_html=True)

    total_listings = len(df) if isinstance(df, pd.DataFrame) else 0
    market_span = "—"
    if data_price_min and data_price_max and data_price_max > data_price_min:
        market_span = f"{data_price_min:,.0f} - {data_price_max:,.0f} THB"
    hero_stats = [
        ("Active leads", f"{total_listings:,}" if total_listings else "—"),
        ("Market span", market_span),
        ("Watchlist", str(saved_count)),
    ]
    stat_html = "<div class='stat-slab'>"
    for label_text, value_text in hero_stats:
        stat_html += f"<div class='stat-card'><label>{label_text}</label><strong>{value_text}</strong></div>"
    stat_html += "</div>"
    st.markdown(stat_html, unsafe_allow_html=True)

    st.markdown("<div class='hero-search-group'>", unsafe_allow_html=True)
    search_cols = st.columns([5, 1, 1], gap="small")
    with search_cols[0]:
        st.markdown("<div class='search-pill-shell'>", unsafe_allow_html=True)
        keyword = st.text_input(
            "Find anything",
            key="keyword_filter",
            placeholder="Search by locations",
            label_visibility="collapsed",
        )
        keyword = (keyword or "").strip()
        st.markdown("</div>", unsafe_allow_html=True)
    with search_cols[1]:
        st.markdown("<div class='round-icon-button'>", unsafe_allow_html=True)
        st.button("Filters", key="filters_trigger", use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
    with search_cols[2]:
        st.markdown("<div class='round-icon-button'>", unsafe_allow_html=True)
        st.button("Voice", key="voice_trigger", use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='category-pills'>", unsafe_allow_html=True)
    category_cols = st.columns(len(hero_categories) + 1, gap="small")
    with category_cols[0]:
        is_active = st.session_state.get('hero_category_active') == "All"
        btn_type = "primary" if is_active else "secondary"
        if st.button("✨ All", key="hero_cat_all", type=btn_type):
            st.session_state['property_type_choice'] = "All"
            st.session_state['property_type_select'] = "All"
            st.session_state['hero_category_active'] = "All"
    for idx, cat in enumerate(hero_categories, start=1):
        with category_cols[idx]:
            target_value = resolve_category_value(cat)
            is_active = st.session_state.get('hero_category_active') == cat['label']
            btn_type = "primary" if is_active else "secondary"
            cat_key = re.sub(r'[^A-Za-z0-9]+', '_', cat['label'])
            clicked = st.button(
                f"{cat['icon']} {cat['label']}",
                key=f"hero_cat_{cat_key}",
                use_container_width=True,
                type=btn_type,
                disabled=target_value is None,
            )
            if clicked:
                if target_value:
                    st.session_state['property_type_choice'] = target_value
                    st.session_state['property_type_select'] = target_value
                    st.session_state['hero_category_active'] = cat['label']
                else:
                    st.session_state['property_type_choice'] = "All"
                    st.session_state['property_type_select'] = "All"
                    st.session_state['hero_category_active'] = "All"
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("<div class='control-bar'>", unsafe_allow_html=True)
    control_cols = st.columns([1.1, 0.9, 0.7, 0.9], gap="small")
    with control_cols[0]:
        default_index = 0 if lang == "English" else 1
        lang = st.selectbox("Language", ["English", "ไทย"], index=default_index, key="lang_select")
    with control_cols[1]:
        show_original = st.checkbox("Show original text", value=show_original, key="show_original")
    with control_cols[2]:
        heart_class = "heart-control saved-active" if st.session_state['view_mode'] == 'saved' else "heart-control"
        st.markdown(f"<div class='{heart_class}'>", unsafe_allow_html=True)
        heart_clicked = st.button(
            f"❤️ {saved_count}",
            key="saved_heart",
            help="View your saved properties",
            use_container_width=True,
            disabled=not username,
        )
        st.markdown("</div>", unsafe_allow_html=True)
        if heart_clicked and username:
            new_mode = 'all' if st.session_state['view_mode'] == 'saved' else 'saved'
            st.session_state['view_mode'] = new_mode
            st.session_state['bottom_nav_active'] = 'Wishlist' if new_mode == 'saved' else 'Home'
        if heart_clicked and not username:
            st.toast("Sign in to manage your wishlist")
    with control_cols[3]:
        if authentication_status:
            if st.button("Logout", key="logout_button_top"):
                logout_user()
        else:
            st.markdown("<div class='nav-chip'>Guest mode</div>", unsafe_allow_html=True)
            with st.popover("Login to save"):
                render_inline_login_controls(inline=True)
    st.markdown("</div>", unsafe_allow_html=True)

    if not authentication_status:
        render_inline_login_controls()

    st.markdown("</div>", unsafe_allow_html=True)

    if isinstance(df, pd.DataFrame) and not df.empty:
        recommend_df = df.head(4)
        if not recommend_df.empty:
            st.markdown(
                "<div class='recommend-title'><h3>Recommended for you</h3><span class='nav-chip'>Fresh intel</span></div>",
                unsafe_allow_html=True,
            )
            cards_html = "<div class='recommend-cards'>"
            for _, rec_row in recommend_df.iterrows():
                card_id = rec_row.get('id')
                card_href = build_query_string(photo=str(card_id), photo_idx="0") if card_id else None
                if not card_href and card_id:
                    card_href = f"?photo={card_id}&photo_idx=0"
                card_href = card_href or "#top-anchor"
                photo_url = extract_primary_photo(rec_row.get('photos')) or "https://images.unsplash.com/photo-1505691938895-1758d7feb511?auto=format&fit=crop&w=800&q=60"
                title_display = clean_text_field(rec_row.get('title_en') or rec_row.get('title'), 'New opportunity')
                location_display = clean_text_field(rec_row.get('location_en') or rec_row.get('location'), 'Location pending')
                price_display = format_price(rec_row.get('price'))
                location_chip = location_display.split('|')[0].strip()
                rating_value = rec_row.get('total_rating') or rec_row.get('investment_rating')
                try:
                    rating_display = f"{float(rating_value):.1f}" if rating_value else "—"
                except (TypeError, ValueError):
                    rating_display = "—"
                beds_display = format_count(rec_row.get('bedrooms')) or format_count(rec_row.get('rooms')) or "—"
                baths_display = format_count(rec_row.get('bathrooms')) or format_count(rec_row.get('bath_count')) or "—"
                meta_bits = []
                if location_chip:
                    meta_bits.append(f"📍 {location_chip}")
                if rating_display != "—":
                    meta_bits.append(f"⭐ {rating_display}")
                if beds_display and beds_display != "—":
                    meta_bits.append(f"🛏 {beds_display} Bed")
                if baths_display and baths_display != "—":
                    meta_bits.append(f"🛁 {baths_display} Bath")
                meta_html = "".join(f"<span class='meta-pill'>{bit}</span>" for bit in meta_bits) or "<span class='meta-pill'>Details loading…</span>"
                badge_text = display_sale_channel((rec_row.get('sale_channel') or 'standard').lower()) or "Best deal"
                is_saved = card_id in saved_ids if card_id else False
                heart_symbol = "♥" if is_saved else "♡"
                heart_class = "recommend-heart saved" if is_saved else "recommend-heart"
                cards_html += compact_html(
                    f"""
                    <a class='recommend-card' href='{card_href}'>
                        <div class='recommend-media'>
                            <img src='{photo_url}' alt='{title_display}' loading='lazy'/>
                            <span class='card-badge'>{badge_text}</span>
                            <span class='{heart_class}'>{heart_symbol}</span>
                        </div>
                        <div class='recommend-body'>
                            <div class='card-headline'>
                                <div>
                                    <p>{location_display}</p>
                                    <h4>{title_display}</h4>
                                </div>
                                <strong class='rec-price'>{price_display}</strong>
                            </div>
                            <div class='card-meta-row'>
                                {meta_html}
                            </div>
                        </div>
                    </a>
                    """
                )
            cards_html += "</div>"
            st.markdown(cards_html, unsafe_allow_html=True)

    st.markdown("<div class='bottom-nav'>", unsafe_allow_html=True)
    nav_items = [
        ("Home", "🏠"),
        ("Insights", "📈"),
        ("Wishlist", "❤️"),
        ("Account", "👤"),
    ]
    nav_cols = st.columns(len(nav_items), gap="small")
    for idx, (nav_label, nav_icon) in enumerate(nav_items):
        is_active = st.session_state.get('bottom_nav_active', 'Home') == nav_label
        btn_type = "primary" if is_active else "secondary"
        clicked = nav_cols[idx].button(f"{nav_icon}  {nav_label}", key=f"bottom_nav_{nav_label}", type=btn_type)
        if clicked:
            st.session_state['bottom_nav_active'] = nav_label
            if nav_label == 'Home':
                st.session_state['view_mode'] = 'all'
                st.session_state['focus_section'] = None
            elif nav_label == 'Wishlist':
                if username:
                    st.session_state['view_mode'] = 'saved'
                else:
                    st.toast("Sign in to open wishlist")
                    st.session_state['bottom_nav_active'] = 'Home'
            elif nav_label == 'Account':
                st.session_state['account_panel_open'] = not st.session_state.get('account_panel_open', False)
            elif nav_label == 'Insights':
                st.session_state['focus_section'] = 'insights'
    st.markdown("</div>", unsafe_allow_html=True)

    if st.session_state.get('account_panel_open'):
        st.markdown("<div id='account-section'></div>", unsafe_allow_html=True)
        with st.container():
            st.markdown(
                """
                <div class='account-panel'>
                    <div class='account-header'>
                        <div>
                            <p class='account-eyebrow'>Control center</p>
                            <h3>Profile & Alerts</h3>
                            <p class='account-subcopy'>Tune your experience for smarter deal flow.</p>
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            account_cols = st.columns([1.2, 1, 1])
            with account_cols[0]:
                st.markdown("### Identity")
                if authentication_status:
                    st.write(f"**User**: {username}")
                    st.write(f"**Saved**: {saved_count}")
                else:
                    st.info("Sign in to sync your wishlist and track saved deals.")
                prev_lang = st.session_state.get('account_language', lang)
                lang_choice = st.radio(
                    "Preferred language",
                    options=["English", "ไทย"],
                    index=0 if prev_lang == "English" else 1,
                    key="account_language_radio",
                )
                st.session_state['account_language'] = lang_choice
                if lang_choice != lang:
                    st.session_state['lang_select'] = lang_choice
            with account_cols[1]:
                st.markdown("### Notifications")
                st.session_state['notify_new_listings'] = st.toggle(
                    "New listing alerts",
                    value=st.session_state.get('notify_new_listings', True),
                    key="notify_new_listings_toggle",
                )
                st.session_state['notify_price_drop'] = st.toggle(
                    "Price drop pings",
                    value=st.session_state.get('notify_price_drop', True),
                    key="notify_price_drop_toggle",
                )
            with account_cols[2]:
                st.markdown("### Preferences")
                st.session_state['dark_mode_pref'] = st.toggle(
                    "Dark mode",
                    value=st.session_state.get('dark_mode_pref', False),
                    key="dark_mode_pref_toggle",
                )
                st.session_state['auto_translate_pref'] = st.toggle(
                    "Auto-translate",
                    value=st.session_state.get('auto_translate_pref', True),
                    key="auto_translate_pref_toggle",
                )

    view_mode = st.session_state['view_mode']
    if view_mode == 'saved':
        st.markdown("<div class='nav-chip'>Saved library</div>", unsafe_allow_html=True)
        if st.button("← Back to listings", key="back_to_all"):
            st.session_state['view_mode'] = 'all'
            view_mode = 'all'
            st.session_state['bottom_nav_active'] = 'Home'


    if df is None or df.empty:
        st.warning("No targets found. Admin needs to run a scan.")
    else:

        def resolve_photo_row(target_id):
            if target_id is None or 'id' not in df.columns:
                return None
            matches = df[df['id'] == target_id]
            if matches is not None and not matches.empty:
                return matches.iloc[0]
            if username:
                fallback = fetch_saved_properties(username)
                if fallback is not None and not fallback.empty and 'id' in fallback.columns:
                    matches = fallback[fallback['id'] == target_id]
                    if not matches.empty:
                        return matches.iloc[0]
            return None

        def render_photo_modal(target_row, target_id, current_idx, language):
            photos = extract_all_photos(target_row.get('photos'))
            current_idx = max(0, min(current_idx, len(photos) - 1))
            native_title = clean_text_field(target_row.get('title'), 'Property')
            native_location = clean_text_field(target_row.get('location'), 'Location unavailable')
            native_description = clean_text_field(target_row.get('description'), '—')
            native_contact = clean_text_field(target_row.get('contact'), '—')
            native_bank = clean_text_field(target_row.get('bank'), '—')

            if language == "English":
                title_display = clean_text_field(target_row.get('title_en')) or native_title
                location_display = clean_text_field(target_row.get('location_en')) or native_location
                description_display = clean_text_field(target_row.get('description_en')) or native_description
                contact_display = clean_text_field(target_row.get('contact_en')) or native_contact
                bank_display = clean_text_field(target_row.get('bank_en')) or native_bank
                description_original = native_description if description_display != native_description else ""
                contact_original = native_contact if contact_display != native_contact else ""
                location_original = native_location if location_display != native_location else ""
            else:
                title_display = native_title
                location_display = native_location
                description_display = native_description
                contact_display = native_contact
                bank_display = native_bank
                description_original = ""
                contact_original = ""
                location_original = ""

            property_type_display = target_row.get('property_type') or normalize_property_type(target_row.get('property_type'), target_row.get('title')) or 'Property'
            property_type_display = clean_text_field(property_type_display, 'Property')
            total_photos = len(photos)
            photo_progress = f"Photo {current_idx + 1}/{total_photos}"
            hero_url = photos[current_idx]
            hero_html = compact_html(
                f"""
                <div class='modal-shell'>
                    <div class='modal-photo-wrapper'>
                        <img src='{hero_url}' alt='property photo {current_idx + 1}' loading='lazy'/>
                        <div class='modal-photo-overlay'>
                            <div>
                                <div class='modal-pill'>{property_type_display}</div>
                                <h3>{title_display}</h3>
                                <p>{location_display}</p>
                            </div>
                            <div class='modal-hint'>{photo_progress} · Use arrows or thumbnails</div>
                        </div>
                    </div>
                </div>
                """
            )

            price_display = format_price(target_row.get('price'))
            size_display = first_valid_measurement([
                ("sqm", target_row.get('size_sqm')),
                ("sqm", target_row.get('usable_area')),
                ("sqm", target_row.get('area_sqm'))
            ])
            land_display = first_valid_measurement([
                ("sqm", target_row.get('land_size_sqm')),
                ("sq.wah", target_row.get('land_size_sq_wah')),
                ("rai", target_row.get('land_size_rai')),
                ("sqm", target_row.get('land_size'))
            ])
            bedroom_count = format_count(target_row.get('bedrooms'))
            room_count = format_count(target_row.get('rooms'))
            beds_display = bedroom_count or room_count or "—"
            if bedroom_count:
                bed_label = l['beds']
            elif room_count:
                bed_label = l['rooms']
            else:
                bed_label = l['beds']
            baths_display = format_count(target_row.get('bathrooms')) or format_count(target_row.get('bath_count')) or "—"
            rent_value = target_row.get('rent_estimate')
            try:
                rent_display = f"{float(rent_value):,.0f} THB / mo" if rent_value else '—'
            except (TypeError, ValueError):
                rent_display = '—'
            investment_rating = target_row.get('investment_rating', '—')

            stat_blocks = [
                (l['size_label'], size_display),
            ]
            if land_display and land_display != '—':
                stat_blocks.append((l['land_size'], land_display))
            stat_blocks.append((bed_label, beds_display))
            stat_blocks.append((l['baths'], baths_display))
            stat_blocks.append(("Price", price_display))
            stats_html = "<div class='detail-info-grid'>"
            for label, value in stat_blocks:
                stats_html += f"<div class='detail-stat'><span>{label}</span><strong>{value}</strong></div>"
            stats_html += "</div>"

            overview_html = f"<p>{description_display}</p>"
            if description_original:
                overview_html += f"<div class='original-note'>Original: {description_original}</div>"
            contact_html = f"<p>{contact_display}</p>"
            if contact_original:
                contact_html += f"<div class='original-note'>Original: {contact_original}</div>"

            detail_links = []
            if target_row.get('url'):
                detail_links.append(f"<a class='cta-pill filled' href='{target_row.get('url')}' target='_blank'>Open BAM listing ↗</a>")
            detail_links_html = "".join(detail_links)

            detail_body_html = compact_html(
                f"""
                <div class='detail-body'>
                    <h4>Full details</h4>
                    {overview_html}
                    <div class='detail-duo'>
                        <div><span>{l['contact']}:</span>{contact_html}</div>
                        <div><span>{l['bank']}:</span><p>{bank_display}</p></div>
                    </div>
                    <div class='detail-duo'>
                        <div><span>{l['rent_estimate']}:</span><p>{rent_display}</p></div>
                        <div><span>{l['investment_rating']}:</span><p>{investment_rating}/10</p></div>
                    </div>
                    <div class='detail-links'>
                        {detail_links_html}
                    </div>
                </div>
                """
            )

            st.markdown("<div class='inline-photo-modal'>", unsafe_allow_html=True)
            st.markdown(hero_html, unsafe_allow_html=True)
            with st.container():
                st.markdown("<div class='modal-nav-grid'>", unsafe_allow_html=True)
                nav_cols = st.columns([1, 1, 1])
                prev_disabled = current_idx == 0
                next_disabled = current_idx >= total_photos - 1
                if nav_cols[0].button("⟵ Previous", disabled=prev_disabled, key=f"modal_prev_{target_id}"):
                    st.query_params["photo_idx"] = str(max(0, current_idx - 1))
                    st.rerun()
                nav_cols[1].markdown(f"<div class='nav-chip modal-chip'>{photo_progress}</div>", unsafe_allow_html=True)
                if nav_cols[2].button("Next ⟶", disabled=next_disabled, key=f"modal_next_{target_id}"):
                    st.query_params["photo_idx"] = str(min(total_photos - 1, current_idx + 1))
                    st.rerun()
                st.markdown("</div>", unsafe_allow_html=True)

            thumbs_html = "<div class='modal-thumb-grid'>"
            for idx, url in enumerate(photos[:12]):
                thumb_href = build_query_string(photo=str(target_id), photo_idx=str(idx)) or f"?photo={target_id}&photo_idx={idx}"
                active_class = "active" if idx == current_idx else ""
                thumbs_html += f"<a class='{active_class}' href='{thumb_href}' target='_self'><img src='{url}' alt='thumbnail {idx + 1}'/></a>"
            thumbs_html += "</div>"
            st.markdown(thumbs_html, unsafe_allow_html=True)
            st.markdown(stats_html, unsafe_allow_html=True)
            st.markdown(detail_body_html, unsafe_allow_html=True)

            st.markdown("<div class='modal-close-row'>", unsafe_allow_html=True)
            if st.button("Close viewer", key=f"modal_close_{target_id}"):
                clear_query_keys("photo", "photo_idx")
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

        def render_pagination_controls(page_key, current_page, total_pages, position="top"):
            if total_pages <= 1:
                return

            def jump_to(target_page):
                st.session_state[page_key] = target_page
                st.rerun()

            sequence = build_pagination_sequence(current_page, total_pages)
            prev_col, pages_col, next_col = st.columns([1, 4, 1])
            with prev_col:
                if st.button("← Prev", key=f"{page_key}_{position}_prev", disabled=current_page <= 1):
                    jump_to(current_page - 1)
            with pages_col:
                page_cols = st.columns(len(sequence))
                for idx, label in enumerate(sequence):
                    with page_cols[idx]:
                        if label == '...':
                            st.markdown("<div style='text-align:center;padding:10px 0;color:var(--text-muted, #94a3b8);'>...</div>", unsafe_allow_html=True)
                        else:
                            is_active = label == current_page
                            if st.button(str(label), key=f"{page_key}_{position}_btn_{label}", disabled=is_active):
                                jump_to(label)
            with next_col:
                if st.button("Next →", key=f"{page_key}_{position}_next", disabled=current_page >= total_pages):
                    jump_to(current_page + 1)

        def render_detail_page(target_row, language):
            property_id = target_row.get('id') or target_row.get('property_id')
            try:
                property_id = int(property_id)
            except (TypeError, ValueError):
                property_id = None

            photos = extract_all_photos(target_row.get('photos'))
            primary_photo = photos[0] if photos else DEFAULT_PLACEHOLDER_IMAGE

            native_title = clean_text_field(target_row.get('title'), 'Property')
            native_location = clean_text_field(target_row.get('location'), 'Location unavailable')
            native_description = clean_text_field(target_row.get('description'), '—')
            native_contact = clean_text_field(target_row.get('contact'), '—')
            native_bank = clean_text_field(target_row.get('bank'), '—')

            if language == "English":
                title_display = clean_text_field(target_row.get('title_en')) or native_title
                location_display = clean_text_field(target_row.get('location_en')) or native_location
                description_display = clean_text_field(target_row.get('description_en')) or native_description
                contact_display = clean_text_field(target_row.get('contact_en')) or native_contact
                bank_display = clean_text_field(target_row.get('bank_en')) or native_bank
                description_original = native_description if description_display != native_description else ""
                contact_original = native_contact if contact_display != native_contact else ""
            else:
                title_display = native_title
                location_display = native_location
                description_display = native_description
                contact_display = native_contact
                bank_display = native_bank
                description_original = ""
                contact_original = ""

            property_type_display = target_row.get('property_type') or normalize_property_type(target_row.get('property_type'), target_row.get('title')) or 'Property'
            property_type_display = clean_text_field(property_type_display, 'Property')
            price_display = format_price(target_row.get('price'))
            bedroom_count = format_count(target_row.get('bedrooms'))
            room_count = format_count(target_row.get('rooms'))
            beds_display = bedroom_count or room_count or "—"
            bed_label = l['beds'] if bedroom_count or not room_count else l['rooms']
            if not bedroom_count and room_count:
                bed_label = l['rooms']
            baths_display = format_count(target_row.get('bathrooms')) or format_count(target_row.get('bath_count')) or "—"
            size_display = first_valid_measurement([
                ("sqm", target_row.get('size_sqm')),
                ("sqm", target_row.get('usable_area')),
                ("sqm", target_row.get('area_sqm'))
            ])
            land_display = first_valid_measurement([
                ("sqm", target_row.get('land_size_sqm')),
                ("sq.wah", target_row.get('land_size_sq_wah')),
                ("rai", target_row.get('land_size_rai')),
                ("sqm", target_row.get('land_size'))
            ])
            rent_value = target_row.get('rent_estimate')
            try:
                rent_display = f"{float(rent_value):,.0f} THB / mo" if rent_value else '—'
            except (TypeError, ValueError):
                rent_display = '—'
            investment_rating = target_row.get('investment_rating', '—')

            sale_channel_value = (target_row.get('sale_channel') or 'standard').lower()
            sale_label = display_sale_channel(sale_channel_value)
            bank_line = f" · {bank_display}" if bank_display and bank_display not in ('—', 'N/A', 'n/a') else ""

            quick_view_href = None
            if property_id is not None:
                quick_view_href = build_query_string(photo=str(property_id), photo_idx='0') or f"?photo={property_id}"

            stat_blocks = [
                (l['price'], price_display),
                (l['size_label'], size_display),
                (l['land_size'], land_display),
                (bed_label, beds_display),
                (l['baths'], baths_display),
                (l['rent_estimate'], rent_display),
                (l['investment_rating'], f"{investment_rating}/10" if investment_rating not in (None, '—') else '—')
            ]

            stats_html = "<div class='detail-info-grid'>"
            for label, value in stat_blocks:
                stats_html += f"<div class='detail-stat'><span>{label}</span><strong>{value}</strong></div>"
            stats_html += "</div>"

            description_html = f"<p>{description_display}</p>"
            if description_original:
                description_html += f"<div class='original-note'>Original: {description_original}</div>"

            contact_html = f"<p>{contact_display}</p>"
            if contact_original:
                contact_html += f"<div class='original-note'>Original: {contact_original}</div>"

            hero_cta_html = ""
            if quick_view_href:
                hero_cta_html = f"<a class='cta-pill filled' href='{quick_view_href}' target='_self'>Quick photo viewer ↗</a>"

            detail_html = compact_html(
                f"""
                <div class='detail-page-shell'>
                    <div class='detail-hero'>
                        <img src='{primary_photo}' alt='detail hero' loading='lazy'/>
                        <div class='detail-hero-overlay'>
                            <div>
                                <div class='modal-pill'>{property_type_display}</div>
                                <h2>{title_display}</h2>
                                <p>{location_display}</p>
                                <div class='sale-pill'>{sale_label}{bank_line}</div>
                            </div>
                            <div class='detail-hero-actions'>
                                {hero_cta_html}
                            </div>
                        </div>
                    </div>
                </div>
                """
            )

            st.markdown(detail_html, unsafe_allow_html=True)

            map_links = build_map_links(target_row.get('lat'), target_row.get('lon'), native_location)
            direction_actions = ""
            if map_links:
                action_buttons = []
                if map_links.get('google'):
                    action_buttons.append(
                        f"<a class='map-icon google' href='{map_links['google']}' target='_blank' rel='noopener noreferrer'><span>G</span></a>"
                    )
                if map_links.get('apple'):
                    action_buttons.append(
                        f"<a class='map-icon apple' href='{map_links['apple']}' target='_blank' rel='noopener noreferrer'><span>A</span></a>"
                    )
                if action_buttons:
                    direction_actions = compact_html(
                        f"""
                        <div class='map-action-row'>
                            <span class='map-action-label'>Directions</span>
                            {''.join(action_buttons)}
                        </div>
                        """
                    )
            if not direction_actions:
                direction_actions = "<div class='map-action-row'><span class='map-action-label'>Directions unavailable</span></div>"

            location_card_html = compact_html(
                f"""
                <div class='detail-location-card'>
                    <div class='location-line'>
                        <span class='location-pill'>{location_display}</span>
                        {f"<div class='original-note'>Original: {location_original}</div>" if location_original else ''}
                    </div>
                    {direction_actions}
                </div>
                """
            )

            st.subheader("Location & Directions")
            st.markdown(location_card_html, unsafe_allow_html=True)
            st.markdown(stats_html, unsafe_allow_html=True)

            st.subheader("Overview")
            st.markdown(description_html, unsafe_allow_html=True)
            st.subheader(l['contact'])
            st.markdown(contact_html, unsafe_allow_html=True)
            if bank_display and bank_display != '—':
                st.caption(f"{l['bank']}: {bank_display}")

            if target_row.get('url'):
                st.markdown(f"[Open full BAM listing ↗]({target_row.get('url')})")

            if photos:
                st.subheader("Photo thumbnails")
                thumb_html = "<div class='modal-thumb-grid'>"
                for idx, url in enumerate(photos[:18]):
                    thumb_href = build_query_string(photo=str(property_id), photo_idx=str(idx)) if property_id is not None else None
                    link_attr = f"href='{thumb_href}' target='_self'" if thumb_href else ""
                    thumb_html += f"<a {link_attr}><img src='{url}' alt='thumbnail {idx + 1}'/></a>"
                thumb_html += "</div>"
                st.markdown(thumb_html, unsafe_allow_html=True)

            if st.button("← Back to listings", key="detail_back_button"):
                clear_query_keys('detail')
                st.rerun()

        params = get_query_params()
        photo_target = params.get('photo')
        if photo_target:
            try:
                target_id = int(photo_target)
            except (TypeError, ValueError):
                target_id = None
            target_row = resolve_photo_row(target_id)
            if target_row is not None:
                try:
                    current_idx = int(params.get('photo_idx', 0))
                except (TypeError, ValueError):
                    current_idx = 0
                render_photo_modal(target_row, target_id, current_idx, lang)

        detail_target = params.get('detail')
        if detail_target:
            try:
                detail_id = int(detail_target)
            except (TypeError, ValueError):
                detail_id = None
            detail_row = resolve_photo_row(detail_id)
            if detail_row is not None:
                render_detail_page(detail_row, lang)
                return
            clear_query_keys('detail')

        total_listings = len(df)
        avg_price = df['price'][df['price'] > 0].mean() if 'price' in df.columns else 0
        auction_count = len(df[df['sale_channel'] == 'auction']) if 'sale_channel' in df.columns else 0
        st.markdown(
            f"""
            <div class='metric-row'>
                <div class='metric-pill'><span>Total Listings</span><h3>{total_listings}</h3></div>
                <div class='metric-pill'><span>Avg Asking</span><h3>{format_price(avg_price)}</h3></div>
                <div class='metric-pill'><span>Auction Deals</span><h3>{auction_count}</h3></div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        sale_display_map = {opt: display_sale_channel(opt) for opt in sale_options}
        sale_label_groups = {}
        for opt, label in sale_display_map.items():
            sale_label_groups.setdefault(label, set()).add(opt)

        sale_filter_label = "All"
        property_type_choice = "All"
        price_min, price_max = data_price_min, data_price_max

        if view_mode == 'all':
            with st.container():
                st.markdown("<div class='filter-card'>", unsafe_allow_html=True)
                top_sale, top_keyword = st.columns([1, 1])
                with top_sale:
                    if sale_options:
                        sale_choices = ["All"] + list(sale_label_groups.keys())
                        sale_filter_label = st.selectbox("Sale channel", sale_choices, key="sale_channel_filter")
                    else:
                        sale_filter_label = "All"
                with top_keyword:
                    active_term = keyword.strip()
                    st.markdown(
                        f"<div class='active-keyword-pill'><span class='pill-icon'>🔍</span>Searching · <strong>{active_term or 'All listings'}</strong></div>",
                        unsafe_allow_html=True,
                    )

                bottom_types, bottom_price = st.columns([2, 1])
                with bottom_types:
                    if property_options:
                        property_choices = ["All"] + property_options
                        previous_choice = st.session_state.get('property_type_choice', "All")
                        if previous_choice not in property_choices:
                            previous_choice = "All"
                        property_type_choice = st.selectbox(
                            l['property_type'],
                            options=property_choices,
                            index=property_choices.index(previous_choice),
                            key="property_type_select",
                        )
                        st.session_state['property_type_choice'] = property_type_choice
                        st.session_state['hero_category_active'] = determine_active_category(property_type_choice)
                    else:
                        property_type_choice = "All"
                        st.caption("No property type data yet")
                        st.session_state['property_type_choice'] = "All"
                        st.session_state['hero_category_active'] = "All"
                with bottom_price:
                    st.caption("Budget (THB)")
                    min_value_default = int(st.session_state.get('budget_min_value', data_price_min))
                    max_value_default = int(st.session_state.get('budget_max_value', data_price_max))
                    min_col, max_col = st.columns(2)
                    with min_col:
                        price_min = st.number_input(
                            "Min",
                            min_value=0,
                            value=min_value_default,
                            step=50000,
                            key="budget_min_input",
                        )
                    with max_col:
                        price_max = st.number_input(
                            "Max",
                            min_value=price_min,
                            value=max(price_min, max_value_default),
                            step=50000,
                            key="budget_max_input",
                        )
                    st.session_state['budget_min_value'] = price_min
                    st.session_state['budget_max_value'] = price_max
                st.markdown("</div>", unsafe_allow_html=True)

        if view_mode == 'all':
            mask = pd.Series(True, index=df.index)
            if 'price' in df.columns:
                mask &= df['price'].fillna(0) >= price_min
                mask &= df['price'].fillna(0) <= price_max

            if property_options and property_type_choice != "All":
                mask &= df['property_type'] == property_type_choice

            if sale_filter_label != "All" and sale_label_groups:
                sale_keys = sale_label_groups.get(sale_filter_label, set())
                if sale_keys:
                    mask &= df['sale_channel'].isin(list(sale_keys))

            if keyword:
                keyword_lower = keyword.lower()
                mask &= df.apply(
                    lambda row: any(
                        keyword_lower in str(row.get(field, '')).lower()
                        for field in ['title', 'location', 'description', 'bank', 'contact']
                    ),
                    axis=1,
                )

            filtered_df = df[mask]
        else:
            filtered_df = df

        if saved_ids and 'id' in df.columns:
            saved_df = df[df['id'].isin(saved_ids)]
        elif saved_ids:
            saved_df = fetch_saved_properties(username)
        else:
            saved_df = pd.DataFrame()

        if not saved_df.empty:
            saved_df = saved_df.copy()
            saved_df['property_type'] = saved_df.apply(lambda row: normalize_property_type(row.get('property_type'), row.get('title')), axis=1)

        def render_property_cards(display_df, context_key, empty_message):
            local_df = display_df.reset_index(drop=True)
            if local_df.empty:
                st.info(empty_message)
                return

            def classify_price_ratio(ratio):
                if ratio is None:
                    return ("No comps yet", "fair")
                if ratio <= 0.85:
                    return ("Below market", "good")
                if ratio <= 1.15:
                    return ("Market price", "fair")
                return ("Premium priced", "bad")

            st.markdown("<div class='property-grid-wrapper'>", unsafe_allow_html=True)
            for block_start in range(0, len(local_df), 3):
                st.markdown("<div class='property-grid-row'>", unsafe_allow_html=True)
                card_cols = st.columns(3, gap="large")
                for offset in range(3):
                    idx = block_start + offset
                    if idx >= len(local_df):
                        break
                    row = local_df.iloc[idx]
                    with card_cols[offset]:
                        property_id = row.get('id') or row.get('property_id')
                        try:
                            property_id = int(property_id)
                        except (TypeError, ValueError):
                            property_id = None

                        native_title = clean_text_field(row.get('title'), 'N/A') or 'N/A'
                        native_location = clean_text_field(row.get('location'), 'N/A') or 'N/A'
                        native_bank = clean_text_field(row.get('bank'), 'N/A') or 'N/A'
                        native_contact = clean_text_field(row.get('contact'), 'N/A') or 'N/A'

                        if lang == "English":
                            title_t = clean_text_field(row.get('title_en')) or translate_text(native_title, "en")
                            location_t = clean_text_field(row.get('location_en')) or translate_text(native_location, "en")
                            bank_t = clean_text_field(row.get('bank_en')) or translate_text(native_bank, "en")
                            contact_t = clean_text_field(row.get('contact_en')) or translate_text(native_contact, "en")
                        else:
                            title_t = native_title
                            location_t = native_location
                            bank_t = native_bank
                            contact_t = native_contact
                        if not location_t:
                            location_t = native_location

                        photos = extract_all_photos(row.get('photos'))
                        first_photo = next(
                            (url for url in photos if looks_like_property_photo(url)),
                            photos[0] if photos else DEFAULT_PLACEHOLDER_IMAGE,
                        )
                        property_type_display = row.get('property_type') or normalize_property_type(row.get('property_type'), row.get('title')) or 'Property'
                        property_type_display = clean_text_field(property_type_display, 'Property')
                        photo_count = len(photos)
                        photo_count_label = f"{photo_count} photo{'s' if photo_count != 1 else ''}"
                        photo_href = "#"
                        if property_id is not None:
                            photo_href = build_query_string(photo=str(property_id), photo_idx='0') or f"?photo={property_id}"

                        raw_price = row.get('price')
                        try:
                            price_value_num = float(raw_price) if raw_price else None
                        except (TypeError, ValueError):
                            price_value_num = None
                        price_display = format_price(raw_price)
                        rent_value = row.get('rent_estimate')
                        try:
                            rent_display = f"{float(rent_value):,.0f} THB / mo" if rent_value else '—'
                        except (TypeError, ValueError):
                            rent_display = '—'
                        living_rating = row.get('living_rating', '—')
                        investment_rating = row.get('investment_rating')
                        try:
                            investment_display = f"{float(investment_rating):.1f}/10" if investment_rating not in (None, '—', '') else '—'
                        except (TypeError, ValueError):
                            investment_display = '—'

                        size_display = first_valid_measurement([
                            ("sqm", row.get('size_sqm')),
                            ("sqm", row.get('usable_area')),
                            ("sqm", row.get('area_sqm'))
                        ])
                        land_display = first_valid_measurement([
                            ("sqm", row.get('land_size_sqm')),
                            ("sq.wah", row.get('land_size_sq_wah')),
                            ("rai", row.get('land_size_rai')),
                            ("sqm", row.get('land_size')),
                            ("sqm", row.get('land_area'))
                        ])
                        bedroom_count = format_count(row.get('bedrooms'))
                        room_count = format_count(row.get('rooms'))
                        beds_value = bedroom_count or room_count or "—"
                        if bedroom_count:
                            bed_meta_label = l['beds']
                        elif room_count:
                            bed_meta_label = l['rooms']
                        else:
                            bed_meta_label = l['beds']
                        bath_count_display = format_count(row.get('bathrooms')) or format_count(row.get('bath_count')) or "—"

                        sale_channel_value = (row.get('sale_channel') or 'standard').lower()
                        sale_label = display_sale_channel(sale_channel_value)
                        bank_display = ''
                        if sale_label.lower().startswith('foreclosure') and bank_t and bank_t.lower() not in ('n/a', 'na', '-'):
                            bank_display = f" · {bank_t}"
                        sale_html = f"<div class='sale-pill'>{sale_label}{bank_display}</div>"

                        detail_href = "#"
                        if property_id is not None:
                            detail_href = build_query_string(
                                detail=str(property_id),
                                photo=None,
                                photo_idx=None
                            ) or f"?detail={property_id}"

                        if not username:
                            save_html = "<span class='icon-btn heart disabled' title='Login to save'></span>"
                        elif property_id is not None:
                            is_saved = property_id in saved_ids
                            save_link = build_query_string(
                                photo=None,
                                photo_idx=None,
                                save=str(property_id),
                                save_op='remove' if is_saved else 'add'
                            ) or f"?save={property_id}&save_op={'remove' if is_saved else 'add'}"
                            heart_class = "icon-btn heart saved" if is_saved else "icon-btn heart"
                            save_html = f"<a class='{heart_class}' href='{save_link}' onclick='event.stopPropagation();' aria-label='Save listing'></a>"
                        else:
                            save_html = ""

                        share_html = f"<a class='icon-btn share' href='{detail_href}' onclick=\"event.stopPropagation();\" aria-label='Share listing'></a>" if property_id is not None else ""

                        photo_html = compact_html(
                            f"""
                            <a class='card-photo' href='{photo_href}' onclick="event.stopPropagation();" target='_self'>
                                <img src='{first_photo}' alt='Photo of {title_t}' loading='lazy'/>
                                <div class='photo-top-row'>
                                    <span class='photo-pill'>{property_type_display}</span>
                                    <div class='photo-actions'>
                                        {save_html}
                                        {share_html}
                                    </div>
                                </div>
                                <div class='photo-bottom-row'>
                                    <span class='photo-pill'>{photo_count_label}</span>
                                    <div class='photo-actions'>
                                        {sale_html}
                                    </div>
                                </div>
                            </a>
                            """
                        )

                        agent_name = contact_t if contact_t and contact_t.lower() not in ("n/a", "na", "-", "—") else "Agent contact pending"
                        agent_meta = bank_t if bank_t and bank_t.lower() not in ("n/a", "na", "-", "—") else sale_label
                        agent_html = compact_html(
                            f"""
                            <div class='agent-chip'>
                                <div class='agent-avatar'>👤</div>
                                <div>
                                    <strong>{agent_name}</strong>
                                    <span>{agent_meta}</span>
                                </div>
                            </div>
                            """
                        )

                        price_block_html = compact_html(
                            f"""
                            <div class='price-cta-row'>
                                <div class='price-stack'>
                                    <span>{l['price']}</span>
                                    <strong>{price_display}</strong>
                                </div>
                                <a class='buy-pill' href='{detail_href}' onclick="event.stopPropagation();">Buy Now</a>
                            </div>
                            """
                        )

                        card_html = compact_html(
                            f"""
                            <div class="property-card">
                                {photo_html}
                                <div class="card-body" role="link" tabindex="0" onclick="window.location='{detail_href}'" onkeypress="if(event.key==='Enter' || event.key===' '){{window.location='{detail_href}'}}">
                                    <h4>{title_t}</h4>
                                    <p class='card-location'>📍 {location_t}</p>
                                    {specs_html}
                                    {agent_html}
                                    {price_block_html}
                                </div>
                            </div>
                            """
                        )
                        st.markdown(card_html, unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

        target_df = saved_df if view_mode == 'saved' else filtered_df
        if view_mode == 'saved' and not username:
            st.info("Login to start a permanent saved list.")

        page_size = 24
        total_items = len(target_df)
        total_pages = max(1, (total_items + page_size - 1) // page_size)
        page_key = f"page_{view_mode}"
        if page_key not in st.session_state or st.session_state[page_key] < 1:
            st.session_state[page_key] = 1
        current_page = st.session_state[page_key]
        if current_page > total_pages:
            current_page = total_pages
            st.session_state[page_key] = current_page

        render_pagination_controls(page_key, current_page, total_pages, position="top")

        start_idx = (current_page - 1) * page_size
        end_idx = start_idx + page_size
        page_df = target_df.iloc[start_idx:end_idx]

        view_label = "Saved" if view_mode == 'saved' else l['listings_found']
        st.subheader(f"{view_label}: {total_items} ({l['page']} {current_page}/{total_pages}) · {page_size} cards/page")
        empty_copy = "You have not saved any properties yet." if view_mode == 'saved' else "No listings match the selected filters."
        render_property_cards(page_df, view_mode, empty_copy)

        current_page = st.session_state.get(page_key, current_page)
        render_pagination_controls(page_key, current_page, total_pages, position="bottom")

        if view_mode == 'all':
            if lang == "English":
                health = st.session_state.get('translation_health', translation_health)
                if not health.get('gcp', True) and not health.get('api', True):
                    warn_msg = health.get('last_error') or "Using backup translator only."
                    st.warning(f"⚠️ Translation services unavailable. {warn_msg}")

            st.markdown("<div id='map-section'></div>", unsafe_allow_html=True)
            st.subheader("📍 Map Overview")
            if not filtered_df.empty and {'lat', 'lon'}.issubset(filtered_df.columns):
                map_columns = ['lat', 'lon', 'title', 'title_en', 'price', 'location', 'location_en', 'photos']
                map_subset = [col for col in map_columns if col in filtered_df.columns]
                map_df = filtered_df[map_subset].dropna(subset=['lat', 'lon']).copy()
                if not map_df.empty:
                    map_df = map_df.head(500)
                    map_df['lat'] = map_df['lat'].astype(float)
                    map_df['lon'] = map_df['lon'].astype(float)

                    map_df['display_title'] = map_df.apply(
                        lambda row: clean_text_field(row.get('title_en')) or clean_text_field(row.get('title')) or "Untitled asset",
                        axis=1
                    )
                    map_df['location_display'] = map_df.apply(
                        lambda row: clean_text_field(row.get('location_en')) or clean_text_field(row.get('location')) or "Location pending",
                        axis=1
                    )
                    if 'price' in map_df.columns:
                        map_df['price_display'] = map_df['price'].apply(
                            lambda value: format_price(value) if pd.notnull(value) else "Price on request"
                        )
                    else:
                        map_df['price_display'] = "Price on request"

                    def resolve_preview_photo(raw_value):
                        if raw_value is None:
                            return DEFAULT_PLACEHOLDER_IMAGE
                        if isinstance(raw_value, float) and math.isnan(raw_value):
                            return DEFAULT_PLACEHOLDER_IMAGE
                        return extract_primary_photo(raw_value)

                    if 'photos' in map_df.columns:
                        map_df['preview_photo'] = map_df['photos'].apply(resolve_preview_photo)
                    else:
                        map_df['preview_photo'] = DEFAULT_PLACEHOLDER_IMAGE

                    zoom_hint = 12 if len(map_df) < 20 else 10 if len(map_df) < 60 else 9
                    def normalize_value(value):
                        if isinstance(value, (str, int, float)) or value is None:
                            return value
                        if hasattr(value, "item"):
                            try:
                                return value.item()
                            except Exception:
                                return str(value)
                        if isinstance(value, (datetime, pd.Timestamp)):
                            return value.isoformat()
                        return str(value)

                    map_records = []
                    for record in map_df.to_dict(orient="records"):
                        map_records.append({key: normalize_value(val) for key, val in record.items()})
                    tooltip = {
                        "html": (
                            "<div style='width:220px'>"
                            "<strong>{display_title}</strong><br/>"
                            "{price_display}<br/>"
                            "<span style='color:#6b7280'>{location_display}</span><br/>"
                            "<img src='{preview_photo}' style='width:100%;margin-top:6px;border-radius:8px;'/>"
                            "</div>"
                        ),
                        "style": {"backgroundColor": "#ffffff", "color": "#111827", "fontSize": "12px"},
                    }
                    deck_spec = {
                        "mapStyle": "mapbox://styles/mapbox/dark-v11",
                        "initialViewState": {
                            "latitude": float(map_df['lat'].mean()),
                            "longitude": float(map_df['lon'].mean()),
                            "zoom": zoom_hint,
                            "pitch": 0,
                        },
                        "layers": [
                            {
                                "@@type": "ScatterplotLayer",
                                "data": map_records,
                                "getPosition": "[lon, lat]",
                                "getRadius": 350,
                                "getFillColor": [239, 68, 68, 180],
                                "getLineColor": [248, 250, 252],
                                "lineWidthMinPixels": 1,
                                "pickable": True,
                            }
                        ],
                        "tooltip": tooltip,
                    }
                    st.pydeck_chart(JsonDeckSpec(deck_spec), width="stretch")
                else:
                    st.caption("No coordinates available yet.")
            else:
                st.caption("No coordinates available yet.")

        nav_items = [
            {"label": "Home", "icon": "🏠"},
            {"label": "Explore", "icon": "🧭"},
            {"label": "Wishlist", "icon": "💖"},
            {"label": "Account", "icon": "👤"},
        ]
        st.markdown("<div class='bottom-app-nav'><div class='nav-shell'>", unsafe_allow_html=True)
        nav_cols = st.columns(len(nav_items), gap="small")
        for col, item in zip(nav_cols, nav_items):
            with col:
                is_active = st.session_state.get('bottom_nav_active', 'Home') == item['label']
                btn_type = "primary" if is_active else "secondary"
                clicked = st.button(
                    f"{item['icon']} {item['label']}",
                    key=f"bottom_nav_{item['label']}",
                    use_container_width=True,
                    type=btn_type,
                )
                if clicked:
                    st.session_state['bottom_nav_active'] = item['label']
                    if item['label'] == 'Home':
                        st.session_state['view_mode'] = 'all'
                        view_mode = 'all'
                        st.session_state['focus_section'] = 'top'
                        st.session_state['account_panel_open'] = False
                    elif item['label'] == 'Explore':
                        st.session_state['view_mode'] = 'all'
                        view_mode = 'all'
                        st.session_state['focus_section'] = 'map'
                        st.session_state['account_panel_open'] = False
                    elif item['label'] == 'Wishlist':
                        st.session_state['view_mode'] = 'saved'
                        view_mode = 'saved'
                        st.session_state['focus_section'] = 'top'
                        st.session_state['account_panel_open'] = False
                    elif item['label'] == 'Account':
                        st.session_state['account_panel_open'] = True
                        st.session_state['focus_section'] = 'account'
        st.markdown("</div></div>", unsafe_allow_html=True)

        focus_target = st.session_state.get('focus_section')
        if focus_target:
            target_id = {
                'top': 'top-anchor',
                'map': 'map-section',
                'account': 'account-section',
            }.get(focus_target)
            if target_id:
                st.markdown(
                    f"""
                    <script>
                        const anchor = document.getElementById('{target_id}');
                        if (anchor) {{ anchor.scrollIntoView({{behavior: 'smooth'}}); }}
                    </script>
                    """,
                    unsafe_allow_html=True,
                )
            st.session_state['focus_section'] = None

main_dashboard()
