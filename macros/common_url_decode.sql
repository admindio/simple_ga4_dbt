{%- macro common_url_decode(decode_me) -%}
  REPLACE(
    REPLACE(
      REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(
              REPLACE(
                REPLACE(
                  REPLACE(
                    REPLACE(
                      REPLACE(
                        REPLACE(
                          REPLACE(
                            REPLACE(
                              REPLACE(
                                REPLACE(
                                  REPLACE(
                                    REPLACE(
                                      REPLACE(
                                        REPLACE(
                                          REPLACE(
                                            REPLACE(
                                              REPLACE(
                                                REPLACE(
                                                  REPLACE(
                                                    REPLACE(
                                                      {{ decode_me }},
                                                      '%7C', '|'),
                                                    '%20', ' '),
                                                  '%21', '!'),
                                                '%2E', '.'),
                                              '%2C', ','),
                                            '%2F', '/'),
                                          '%3A', ':'),
                                        '%3B', ';'),
                                      '%3F', '?'),
                                    '%40', '@'),
                                  '%23', '#'),
                                '%24', '$'),
                              '%25', '%'),
                            '%26', '&'),
                          '%27', "'"),
                        '%28', '('),
                      '%29', ')'),
                    '%2A', '*'),
                  '%2B', '+'),
                '%3D', '='),
              '%5B', '['),
            '%5D', ']'),
          '%7B', '{'),
        '%7D', '}'),
      '%3C', '<'),
    '%3E', '>')
{%- endmacro -%}