-- set target directory
set_targetdir("$(buildir)/bench")

-- define target
target("bench")

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

