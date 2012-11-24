require 'rdoc/task'

RDOC_FILES = FileList['README.rdoc', 'LICENSE', 'ext/nuodb/nuodb.cpp', 'lib/**/*.rb']

require 'rdoc/task'
Rake::RDocTask.new do |rdoc|
  rdoc.title = "#{name} #{version}"
  rdoc.main = 'README.rdoc'
  rdoc.rdoc_dir = 'doc/site/api'
  rdoc.options << "-a" << "-U" << "-D" << "-v" << '-f' << 'hanna'
  rdoc.rdoc_files.include(RDOC_FILES)
end

Rake::RDocTask.new(:ri) do |rdoc|
  rdoc.main = "README.rdoc"
  rdoc.rdoc_dir = "doc/ri"
  rdoc.options << "--ri-system"
  rdoc.rdoc_files.include(RDOC_FILES)
end
