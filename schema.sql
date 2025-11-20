-- USERS TABLE: Handles authentication and roles
CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY,
    password TEXT NOT NULL,  -- Plain text for MVP, hash for production
    role TEXT DEFAULT 'client', -- 'admin' or 'client'
    is_active BOOLEAN DEFAULT TRUE,
    remember_token TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- PROPERTIES TABLE: Stores the assets
CREATE TABLE IF NOT EXISTS properties (
    id SERIAL PRIMARY KEY,
    source TEXT, -- e.g., 'BAM', 'SAM'
    title TEXT,
    price NUMERIC,
    size_sqm NUMERIC,
    lat FLOAT,
    lon FLOAT,
    url TEXT UNIQUE, -- Prevents duplicates
    image_url TEXT,
    photos TEXT,
    property_type TEXT,
    sale_channel TEXT,
    description TEXT,
    description_en TEXT,
    title_en TEXT,
    location TEXT,
    location_en TEXT,
    contact TEXT,
    contact_en TEXT,
    bank TEXT,
    bank_en TEXT,
    living_rating NUMERIC,
    rent_estimate NUMERIC,
    investment_rating NUMERIC,
    rooms NUMERIC,
    bedrooms NUMERIC,
    bathrooms NUMERIC,
    status TEXT DEFAULT 'active',
    last_seen_at TIMESTAMP DEFAULT NOW(),
    
    -- SNIPER METRICS
    strategy TEXT, -- 'Cash Flow' or 'Big Flip'
    total_rating NUMERIC, -- 0-10 Score
    safety_score INT,
    transport_score INT,
    food_score INT,
    
    last_updated TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS properties_price_history (
    id SERIAL PRIMARY KEY,
    url TEXT REFERENCES properties(url) ON DELETE CASCADE,
    price NUMERIC,
    recorded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bam_feed_snapshot (
    id SERIAL PRIMARY KEY,
    feed_type TEXT NOT NULL,
    category TEXT NOT NULL,
    total_records INT,
    page_count INT,
    checked_at TIMESTAMP DEFAULT NOW()
);

-- SEED DATA: Create the Super Admin
INSERT INTO users (username, password, role) 
VALUES ('admin', 'admin123', 'admin') 
ON CONFLICT DO NOTHING;