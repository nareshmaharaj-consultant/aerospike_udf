---
--- Generated by EmmyLua(https://github.com/EmmyLua)
--- Created by nareshmaharaj.
--- DateTime: 28/06/2021 12:22
---

---validateBeforeWrite
--- Stored on server /opt/aerospike/usr/udf/lua
---@param r table
---@param bin1 table
---@param bin2 table
---@param value1 table
---@param value2 table
function validateBeforeWrite( r, bin1, bin2, bin3, value1, value2, value3 )
    if ( value1 > 0 and value1 < 110 ) then
        if not aerospike:exists(r) then
            r["CreationDate"] = os.date()
            aerospike:create(r)
        end
        r[bin1] = value1
        r[bin2] = value2
        r[bin3] = value3
        r["updateTime"] = os.date()
        aerospike:update(r)
    else
        error("1000: Invalid value")
    end
end