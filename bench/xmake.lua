-- set target directory
set_targetdir("$(buildir)/bench")

-- define target
target("bench")

    -- do not build by default
    set_default(false)

    -- set kind
    set_kind("static")

    -- add deps: libxredis.a
    add_deps("xredis")

    -- add files
    add_files("xRdbBench.cpp")
    add_files("xBufferBench.cpp")
    add_files("xThreadPoolBench.cpp")
    add_files("xLogAsyncBench.cpp")
    add_files("xRoundTripUdp.cpp")
    add_files("xTimerBench.cpp")
    add_files("xClusterBench.cpp")
    add_files("xReplicationBench.cpp")

-- define targets
for _, name in ipairs({"xHiredisClient", "xServer", "xBench", "xHiredisSync",
                       "xHiredisAsync", "xLogBench", "xRoundTrip", "xSdsBench", "xRedisBenchMark", "xClient"}) do
    target(name)

        -- do not build by default
        set_default(false)

        -- set kind
        set_kind("binary")

        -- add deps: libbench.a
        add_deps("bench")

        -- add files
        add_files(name .. ".cpp")

        -- add links
        if name == "xHiredisClient" then
            add_links("hiredis")
        end
end

-- define targets
target("benchmark")
    set_default(false)
    add_deps("xHiredisClient", "xServer", "xBench")
    add_deps("xHiredisSync", "xHiredisAsync", "xLogBench")
    add_deps("xRoundTrip", "xSdsBench", "xRedisBenchMark", "xClient")
