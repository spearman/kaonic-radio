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
    (python3.withPackages (p: with p; [
      ipython
      python-lsp-server
    ]))
    rustup
    rust-analyzer
  ];
  LIBCLANG_PATH="${lib.getLib llvmPackages.libclang}/lib";
  LD_LIBRARY_PATH = lib.makeLibraryPath [
    libglvnd
    libX11
    libXcursor
    libXi
    libXrandr
    libxkbcommon
  ];
}
