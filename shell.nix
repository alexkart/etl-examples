let
    pkgs = import <nixpkgs> {};
in

let
    php = pkgs.php81.buildEnv {
        extraConfig =
        ''
            short_open_tag = Off
            memory_limit = -1
            date.timezone = UTC
        '';
    };
in

pkgs.mkShell {
    name = "flow-php-examples";
    nativeBuildInputs = with pkgs; [
        php
        php.packages.composer
    ];
}