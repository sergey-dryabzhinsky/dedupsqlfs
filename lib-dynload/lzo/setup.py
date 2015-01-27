from distutils.core import setup, Extension

setup(
    name = "LZO",
    version = "1.0",
    ext_modules = [
        Extension(
            "lzo",
            ["lzomodule.c"],
            libraries=['lzo2'],
            extra_compile_args=["-O2", "-DFORTIFY_SOURCE=2", "-fstack-protector"]
#            extra_compile_args=["-O2", "-march=native"]
#            extra_compile_args=["-O2", "-march=native", "-floop-interchange", "-floop-block", "-floop-strip-mine", "-ftree-loop-distribution"]
        )
    ]
)
