-- h2_square_bracket_footnotes.lua
-- Converts Word footnote markers from superscripted numbers to [n] style in text,
-- while preserving real footnotes in DOCX output.

function Note (note)
  local num = tostring(pandoc.utils.stringify(note))
  -- Replace inline footnote with square-bracketed reference
  return pandoc.RawInline('openxml',
      string.format(
        '<w:r><w:t xml:space="preserve">[%s]</w:t></w:r>',
        pandoc.utils.stringify(#PANDOC_STATE.notes + 1)
      )
  )
end
