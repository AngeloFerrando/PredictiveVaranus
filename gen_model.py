import spot

# LTL formula
formula = "G(a -> F b)"

# Parse LTL formula
f = spot.formula(formula)

# Translate LTL to Büchi automaton
aut = spot.translate(f, 'BA')

# Save automaton in HOA format
with open("model.hoa", "w") as f:
    f.write(aut.to_str("hoa"))

print("Automaton saved to model.hoa")