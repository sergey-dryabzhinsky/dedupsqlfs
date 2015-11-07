from distutils.core import setup, Extension

setup(
    name = "LZO",
    version = "1.0",
    ext_modules = [
        Extension(
            "lzo",
            ["lzomodule.c"],
            libraries=['lzo2'],
            extra_compile_args=[
                "-O3",
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
