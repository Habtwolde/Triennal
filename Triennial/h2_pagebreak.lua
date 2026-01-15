-- Insert a DOCX page break before the second level-2 header
local h2_count = 0
function Header(el)
  if el.level == 2 then
    h2_count = h2_count + 1
    if h2_count == 2 then
      return { pandoc.RawBlock('openxml','<w:p><w:r><w:br w:type="page"/></w:r></w:p>'), el }
    end
  end
  return nil
end
