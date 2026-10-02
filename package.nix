{ lib, stdenvNoCC, python3 }:
stdenvNoCC.mkDerivation {
  pname = "cliproxyapi-usage";
  version = "0.1.0";
  src = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.unions [ ./cliproxyapi_usage.py ./test_cliproxyapi_usage.py ];
  };
  buildInputs = [ python3 ];
  nativeCheckInputs = [ python3 ];
  doCheck = true;
  checkPhase = ''
    runHook preCheck
    python3 -m unittest -v test_cliproxyapi_usage
    runHook postCheck
  '';
  installPhase = ''
    runHook preInstall
    install -Dm755 cliproxyapi_usage.py $out/bin/cliproxyapi-usage
    runHook postInstall
  '';
  meta = {
    description = "Usage history and dashboard for CLIProxyAPI";
    homepage = "https://github.com/maarao/cliproxyapi-usage";
    license = lib.licenses.mit;
    mainProgram = "cliproxyapi-usage";
    platforms = lib.platforms.unix;
  };
}
