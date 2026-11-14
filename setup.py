"""Build configuration for the optional native C99 Sentinel extension."""

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext


class _BuildExt(build_ext):
    """Apply compiler-appropriate optimization and visibility flags."""

    def build_extensions(self) -> None:
        compiler_type = getattr(self.compiler, "compiler_type", "")
        compile_args = (
            ["/O2", "/std:c11"]
            if compiler_type == "msvc"
            else ["-std=c99", "-O3", "-fvisibility=hidden"]
        )
        for ext in self.extensions:
            ext.extra_compile_args = [*ext.extra_compile_args, *compile_args]
        super().build_extensions()


_SENTINEL_EXTENSION = Extension(
    "webaiprovider.providers.chatgpt.sentinel._native",
    sources=[
        "sentinel/_native.c",
        "sentinel/base64.c",
        "sentinel/clock.c",
        "sentinel/fnv1a.c",
        "sentinel/pow.c",
        "sentinel/xor.c",
    ],
    include_dirs=["sentinel"],
    define_macros=[("SENTINEL_STATIC", "1")],
    optional=True,
)

setup(
    ext_modules=[_SENTINEL_EXTENSION],
    cmdclass={"build_ext": _BuildExt},
)
