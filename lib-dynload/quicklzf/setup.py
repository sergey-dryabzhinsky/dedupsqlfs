from distutils.core import setup, Extension

setup(
    name = "quicklz",
    version = "1.0",
    packages=[],
    package_dir={'': 'src'},
    ext_modules = [
        Extension(
            "quicklz",
            ["src/quicklz.c", "src/quicklzpy.c"],
            extra_compile_args=[
                "-O3",
                "-DQLZ_COMPRESSION_LEVEL=1", "-DQLZ_STREAMING_BUFFER=%s" % (64*1024,),
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
