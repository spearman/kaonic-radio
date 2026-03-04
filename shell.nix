with import <nixpkgs> {};
mkShell {
  buildInputs = [
    atkmm
    clang
    gdb # required for rust-gdb
    glib
    gtk3
    libgpiod
    openssl
    pkg-config
    protobuf
    rustup
    rust-analyzer
  ];
  LIBCLANG_PATH="${lib.getLib llvmPackages.libclang}/lib";
  LD_LIBRARY_PATH = lib.makeLibraryPath [
    libglvnd
    xorg.libX11
    xorg.libXcursor
    xorg.libXi
    xorg.libXrandr
    libxkbcommon
  ];
}
