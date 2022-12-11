---
--- Generated by EmmyLua(https://github.com/EmmyLua)
--- Created by nareshmaharaj.
--- DateTime: 28/06/2021 12:22
---

---validateBeforeWrite
--- Stored on server /opt/aerospike/usr/udf/lua
---@param rec table
---@param bin1 table
---@param bin2 table
---@param value1 table
---@param value2 table
function validateBeforeWrite( rec, bin1, bin2, bin3, value1, value2, value3 )
    if ( value1 > 0 and value1 < 110 ) then
        if not aerospike:exists(rec) then
            rec["creationDate"] = os.time() * 1000
            aerospike:create(rec)
        end
        rec[bin1] = value1
        rec[bin2] = value2
        rec[bin3] = value3
        rec["updateTime"] = os.time() * 1000
        aerospike:update(rec)
    else
        error("1000: Invalid value")
    end
end

function totals(rec, unitsSold, mfgPrice, salesPrice, queryFieldValue, queryFieldBinName)
    local unitsSold = tonumber( rec[unitsSold] )
    local salesPrice = tonumber( rec[salesPrice] )
    local totalSales = unitsSold * salesPrice
    rec["totalSales"] = totalSales

    local mfgPrice = rec[mfgPrice]
    local totalCost = mfgPrice * unitsSold
    rec["totalCost"] = totalCost

    local profit = totalSales - totalCost
    rec["profit"] = profit

    rec["profitMargin"] = math.floor( profit / totalSales * 100 )
    local taxRates = vatRate(rec["country"]) or 20

    rec["taxRates"] = taxRates

    local taxDue = taxRates * profit / 100
    if ( taxDue < 0 ) then
        taxDue = 0
    end
    rec["taxDue"] = taxDue

    --queryFieldValue required as we cant use Exp with queryAggregate for fine grained filter.
    if rec[queryFieldBinName] == nil then
        local m = map()
        m[queryFieldValue]=1
        rec[queryFieldBinName] = m
    else
        local m = rec[queryFieldBinName]
        m[queryFieldValue] =1
        rec[queryFieldBinName] = m
    end
    return aerospike:update(rec)
end

function vatRate(country)
    vatRates = {}
    vatRates["Canada"] = 26.5
    vatRates["France"] = 26.5
    vatRates["Germany"] = 30
    vatRates["UnitedStatesofAmerica"] = 21
    vatRates["Mexico"] = 30
    return vatRates[country]
end

-- REDUCER
local function reduce_stream(a, b)
    local out = map.merge(
            a,b,
            function( v1, v2)
                return round( v1 + v2, 2 )
            end
    )
    return out
end

-- SALES
local function aggregate_sales(out, rec)
    if out[ rec["country"] ] == nil then
        out[ rec["country"] ] = ( rec["totalSales"] or 0 )
    else
        out[ rec["country"] ] = out[ rec["country"] ]  + ( rec["totalSales"] or 0 )
    end
    return out
end

-- VAT DUE
local function aggregate_vatDue(out, rec)
    if out[ rec["country"] ] == nil then
        out[ rec["country"] ] = ( rec["taxDue"] or 0 )
    else
        out[ rec["country"] ] = out[ rec["country"] ]  + ( rec["taxDue"] or 0 )
    end
    return out
end

function calculateSales(stream)
    return stream                                     :
    aggregate( map{ country = nil }, aggregate_sales): reduce(reduce_stream)
end

function calculateVatDue(stream)
    return stream                                     :
    aggregate( map{ country = nil }, aggregate_vatDue): reduce(reduce_stream)
end

function round(num, numDecimalPlaces)
    local mult = 10^(numDecimalPlaces or 0)
    if num >= 0 then return math.floor(num * mult + 0.5) / mult
    else return math.ceil(num * mult - 0.5) / mult end
end

function slice(rec, bin, a, b)
    local s = rec[bin]
    if type(s) == 'string' then
        local subs = s:sub(a, b)
        rec[bin] = subs
        return aerospike:update(rec)
    end
    return nil
end