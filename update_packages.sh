#!/bin/bash

# Update package declarations
find src/main/java/com/jparque/columnar -name "*.java" -type f -exec sed -i '' 's/package com\.jparque\.\(chunk\|page\|rowgroup\|file\|serialization\|deserialization\)/package com.jparque.columnar/g' {} +

# Update imports
find src/main/java/com/jparque -name "*.java" -type f -exec sed -i '' 's/import com\.jparque\.\(chunk\|page\|rowgroup\|file\|serialization\|deserialization\)/import com.jparque.columnar/g' {} +

# Update common package declarations
find src/main/java/com/jparque/common -name "*.java" -type f -exec sed -i '' 's/package com\.jparque\.\(compression\|schema\|metadata\)/package com.jparque.common/g' {} +

# Update common imports
find src/main/java/com/jparque -name "*.java" -type f -exec sed -i '' 's/import com\.jparque\.\(compression\|schema\|metadata\)/import com.jparque.common/g' {} +
