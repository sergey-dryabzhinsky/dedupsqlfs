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
                "-march=native",
                "-DFORTIFY_SOURCE=2", "-fstack-protector",
#                "-floop-interchange", "-floop-block", "-floop-strip-mine", "-ftree-loop-distribution"
            ]
        )
    ]
)
