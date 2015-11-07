from distutils.core import setup, Extension

setup(
    name = "quicklz",
    version = "1.0",
    ext_modules = [
        Extension(
            "quicklz",
            ["quicklz.c", "quicklzpy.c"],
            extra_compile_args=[
                "-O3",
                "-DQLZ_COMPRESSION_LEVEL=2", "-DQLZ_STREAMING_BUFFER=%s" % (256*1024,),
# Hardening
            "-DFORTIFY_SOURCE=2", "-fstack-protector",
# Full CPU optimization, for custom build by hand
#            "-march=native",
# GCC Graphite
#            "-floop-interchange", "-floop-block", "-floop-strip-mine", "-ftree-loop-distribution",
            ]
        )
    ]
)
