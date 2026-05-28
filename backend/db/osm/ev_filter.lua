-- osm2pgsql Lua filter for EV charging stations
-- Converts OSM amenity=charging_station nodes into our schema

local tag_keys = {
    "amenity",
    "name",
    "operator",
    "capacity",
    "socket:type2",
    "socket:type2_combo",
    "socket:chademo",
    "socket:type3",
    "socket:ccs",
    "socket:tesla",
}

function filter_tags_node(keyvalues, keys, values)
    if keyvalues["amenity"] == "charging_station" then
        keys["amenity"] = "charging_station"
        values["operator"] = keyvalues["operator"] or "Unknown"
        values["name"] = keyvalues["name"] or values["operator"] .. " Charging Station"
        values["capacity"] = keyvalues["capacity"] or "1"
        values["socket:type2"] = keyvalues["socket:type2"]
        values["socket:type2_combo"] = keyvalues["socket:type2_combo"]
        values["socket:chademo"] = keyvalues["socket:chademo"]
        values["socket:type3"] = keyvalues["socket:type3"]
        values["socket:ccs"] = keyvalues["socket:ccs"]
        values["socket:tesla"] = keyvalues["socket:tesla"]
        return 1, keys, values
    end
    return 0, keys, values
end

function osm2pgsql_process_node(object)
    if object.tags.amenity == "charging_station" then
        local station_id = "stn-" .. object.osm_id
        -- Truncate name
        local name = object.tags.name or (object.tags.operator or "Unknown") .. " Charging Station"
        local operator = object.tags.operator or "Unknown"

        -- Insert into stations table (partner_id defaults to a generic OSM partner)
        object:insert(string.format([[
            INSERT INTO stations (id, name, partner_id, geom, status, is_live, updated_at)
            VALUES (
                '%s',
                %s,
                (SELECT id FROM partners WHERE name = 'OSM Import' LIMIT 1),
                ST_SetSRID(ST_MakePoint(%.7f, %.7f), 4326)::geography,
                'Available',
                false,
                NOW()
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                geom = EXCLUDED.geom;
        ]], station_id, name, object.lon, object.lat))

        -- Insert chargers based on socket tags
        local sockets = {
            { tag = "socket:type2",       plug = "Type2",  power = 22 },
            { tag = "socket:type2_combo", plug = "CCS2",   power = 50 },
            { tag = "socket:chademo",     plug = "CHAdeMO", power = 50 },
            { tag = "socket:ccs",         plug = "CCS2",   power = 120 },
            { tag = "socket:type3",       plug = "Type3",  power = 22 },
            { tag = "socket:tesla",       plug = "Tesla",  power = 120 },
        }

        local charger_idx = 0
        for _, s in ipairs(sockets) do
            if object.tags[s.tag] and object.tags[s.tag] ~= "0" then
                charger_idx = charger_idx + 1
                local charger_id = "chg-" .. object.osm_id .. string.sub("000" .. charger_idx, -3)
                object:insert(string.format([[
                    INSERT INTO chargers (id, station_id, plug_type, power_output, status, is_live, updated_at)
                    VALUES ('%s', '%s', '%s', %d, 'Available', false, NOW())
                    ON CONFLICT (id) DO NOTHING;
                ]], charger_id, station_id, s.plug, s.power))
            end
        end

        -- Fallback: if no socket tags found, insert one generic charger
        if charger_idx == 0 then
            local charger_id = "chg-" .. object.osm_id .. "001"
            object:insert(string.format([[
                INSERT INTO chargers (id, station_id, plug_type, power_output, status, is_live, updated_at)
                VALUES ('%s', '%s', 'Type2', 22, 'Available', false, NOW())
                ON CONFLICT (id) DO NOTHING;
            ]], charger_id, station_id))
        end
    end
end
