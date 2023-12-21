
#include "operators/operators.h"

#include <cassert>
#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "common/macros.h"

#define UNUSED(p) ((void)(p))
namespace buzzdb {
namespace operators {

Register Register::from_int(int64_t value) {
  Register reg;
  reg.value.emplace<0>(value);
  return reg;
}

Register Register::from_string(const std::string& value) { 
  Register reg;
  reg.value.emplace<1>(value);
  return reg;
}

Register::Type Register::get_type() const {
    if (value.index() == 0) 
      return Type::INT64;
    else
      return Type::CHAR16;
}

int64_t Register::as_int() const {
    return std::get<0>(value);
}

std::string Register::as_string() const {
    return std::get<1>(value);
}

uint64_t Register::get_hash() const {
    return std::hash<decltype(value)>{}(value);
}

bool operator==(const Register& r1, const Register& r2) {
    return r1.value == r2.value; 
}

bool operator!=(const Register& r1, const Register& r2) {
    return r1.value != r2.value; 
}

bool operator<(const Register& r1, const Register& r2) {
    assert(r1.get_type() == r2.get_type());
    if (r1.get_type() == Register::Type::INT64) {
        return r1.as_int() < r2.as_int();
    } else {
        assert(r1.get_type() == Register::Type::CHAR16);
        return r1.as_string().compare(r2.as_string()) < 0;
    }
}

bool operator<=(const Register& r1, const Register& r2) {
    assert(r1.get_type() == r2.get_type());
    return r1 < r2 || r1 == r2;
}

bool operator>(const Register& r1, const Register& r2) {
  assert(r1.get_type() == r2.get_type());
  if (r1.get_type() == Register::Type::INT64) {
    return r1.as_int() > r2.as_int();
  } else {
    assert(r1.get_type() == Register::Type::CHAR16);
    return r1.as_string().compare(r2.as_string()) > 0;
  }
}

bool operator>=(const Register& r1, const Register& r2) {
  assert(r1.get_type() == r2.get_type());
  return r1 > r2 || r1 == r2;
}

Print::Print(Operator& input, std::ostream& stream) : UnaryOperator(input), stream(stream) {}

Print::~Print() = default;

void Print::open() {
    input->open();
    input_regs = input->get_output();
}

bool Print::next() {
    if (input->next()) {
    std::vector<Register*> input_tuple = input->get_output();
    size_t reg_itr = 0;
    for (size_t i = 0; i < input_tuple.size(); i++) {
      if (input_tuple.at(i)->get_type() == Register::Type::INT64) {
        stream << input_tuple.at(i)->as_int();
      } else if (input_tuple.at(i)->get_type() == Register::Type::CHAR16) {
        stream << input_tuple.at(i)->as_string();
      }
      if (reg_itr++ != input_tuple.size() - 1) {
        stream << ',';
      }
    }
    stream.put('\n');
    return true;
  }
  return false;
}

void Print::close() {
    input->close();
    stream.clear();
}

std::vector<Register*> Print::get_output() {
    /// Print has no output.
    return {};
}

Projection::Projection(Operator& input, std::vector<size_t> attr_indexes) 
: UnaryOperator(input), attr_indexes(std::move(attr_indexes)) {}

Projection::~Projection() = default;

void Projection::open() {
  input->open();
}

bool Projection::next() {
  return input->next();
}

void Projection::close() {
  input->close();
  attr_indexes.clear();
}

std::vector<Register*> Projection::get_output() {
  const std::vector<Register*>& src_regs = input->get_output();
  std::vector<Register*> output;
  size_t i = 0;
  while (i < attr_indexes.size()) {
    output.push_back(src_regs[i]);
    i++;
  }
  return output;
}

Select::Select(Operator& input, PredicateAttributeInt64 predicate)  // NOLINT
  : UnaryOperator(input),
  attr_index(predicate.attr_index),
  right_operand{std::in_place_index<0>, Register::from_int(predicate.constant)},
  predicate_type(predicate.predicate_type) {}

Select::Select(Operator& input, PredicateAttributeChar16 predicate)  // NOLINT
  : UnaryOperator(input),
  attr_index(predicate.attr_index),
  right_operand{std::in_place_index<0>, Register::from_string(predicate.constant)},
  predicate_type(predicate.predicate_type) {}

Select::Select(Operator& input, PredicateAttributeAttribute predicate)  // NOLINT
  : UnaryOperator(input),
  attr_index(predicate.attr_left_index),
  right_operand{std::in_place_index<1>, predicate.attr_right_index},
  predicate_type(predicate.predicate_type) {}

Select::~Select() = default;

void Select::open() {
    input->open();
    input_regs = input->get_output();
}

bool Select::next() {
    while (input->next()) {
        bool result = [&]() {
        auto* reg_left = input_regs[attr_index];
        Register* reg_right;
        if (right_operand.index() == 0) {
            reg_right = &std::get<0>(right_operand);
        } else if (right_operand.index() == 1) {
            reg_right = input_regs[std::get<1>(right_operand)];
        }
        if (predicate_type == PredicateType::EQ)
            return (*reg_left == *reg_right);
        else if (predicate_type == PredicateType::NE)
            return (*reg_left != *reg_right);
        else if (predicate_type == PredicateType::LT)
            return (*reg_left < *reg_right);
        else if (predicate_type == PredicateType::LE)
            return (*reg_left <= *reg_right);
        else if (predicate_type == PredicateType::GT)
            return (*reg_left > *reg_right);
        else if (predicate_type == PredicateType::GE)
            return (*reg_left >= *reg_right);
        else
            __builtin_unreachable();
      }();

        if (result) {
            return true;
        } 
    }
    return false;
}

void Select::close() {
  input->close(); 
}

std::vector<Register*> Select::get_output() {
    return input->get_output(); 
}

Sort::Sort(Operator& input, std::vector<Criterion> criteria) : UnaryOperator(input), criteria(std::move(criteria)) {}

Sort::~Sort() = default;

void Sort::open() {
    input->open();
    input_regs = input->get_output();
    output_regs.resize(input_regs.size());
}

bool Sort::next() {
    if (next_output_offset == 0) {
        while (input->next()) {
            std::vector<Register> reg;
            for (const auto& attr : input_regs) {
                reg.push_back(*attr);
            }
            sorted.push_back(reg);
        }
        auto i = criteria.rbegin();
        while (i != criteria.rend()) {
            const auto& criterion = *i;
            std::stable_sort(sorted.begin(), sorted.end(), [&criterion](const std::vector<Register>& a, const std::vector<Register>& b) {
                if (!criterion.desc) {
                    return a[criterion.attr_index] < b[criterion.attr_index];
                } else {
                    return a[criterion.attr_index] > b[criterion.attr_index];
                }
            });
            i++;
        }
    }
    if (next_output_offset != sorted.size()) {
        output_regs = sorted[next_output_offset++];
        return true;
    } else {
        return false;
    }
}

std::vector<Register*> Sort::get_output() { 
  std::vector<Register*> output;
  for (auto& reg : output_regs) {
      output.push_back(&reg);
  }
  return output;
}

void Sort::close() {
    input->close();
    sorted.clear();
}

HashJoin::HashJoin(Operator& input_left, Operator& input_right, size_t attr_index_left, size_t attr_index_right)
    : BinaryOperator(input_left, input_right), attr_index_left(attr_index_left), attr_index_right(attr_index_right) {}

HashJoin::~HashJoin() = default;

void HashJoin::open() {
    input_left->open();
    input_right->open();
    input_regs_left = input_left->get_output();
    input_regs_right = input_right->get_output();
    output_regs.resize(input_regs_left.size() + input_regs_right.size());
}

bool HashJoin::next() {
    if (!ht_build) {
        while (input_left->next()) {
            const auto &input_tuple = input_left->get_output();
            std::vector<Register> reg;
            for (const auto &attr : input_tuple) {
                reg.push_back(*attr);
            }
            ht.emplace(*input_tuple[attr_index_left], reg);
        }
    }
    while (input_right->next()) {
        const auto it = ht.find(*input_regs_right[attr_index_right]);
        if (it != ht.end()) {
            size_t i = 0;
            while (i < it->second.size()) {
                output_regs[i] = it->second[i];
                i++;
            }
            i = 0;
            while (i < input_regs_right.size()) {
                output_regs[it->second.size() + i] = *input_regs_right[i];
                i++;
            }
            return true;
        }
    }
    return false;
}

void HashJoin::close() {
    input_left->close();
    input_right->close();
    ht.clear();
}

std::vector<Register*> HashJoin::get_output() {
  std::vector<Register*> output;;
  for (auto& reg : output_regs) {
      output.push_back(&reg);
  }
  return output; 
}

HashAggregation::HashAggregation(Operator& input, std::vector<size_t> group_by_attrs, std::vector<AggrFunc> aggr_funcs) :
    UnaryOperator(input), group_by_attrs(std::move(group_by_attrs)), aggr_funcs(std::move(aggr_funcs)) {}

HashAggregation::~HashAggregation() = default;

void HashAggregation::open() {
    input->open();
    input_regs = input->get_output();
    output_regs.resize(aggr_funcs.size() + group_by_attrs.size());
}

bool HashAggregation::next() {
    if (seen_input) {
        ++output_iterator;
    } else {
        while (input->next()) {
            std::vector<Register> group_by_regs;
            for (const auto index : group_by_attrs) {
                group_by_regs.push_back(*input_regs[index]);
            }
            auto& agggregates = ht[group_by_regs];
            if (agggregates.empty()) {
                for (const auto& aggr_func : aggr_funcs) {
                    switch (aggr_func.func) {
                    case AggrFunc::Func::MIN:
                    case AggrFunc::Func::MAX:
                        agggregates.push_back(*input_regs[aggr_func.attr_index]);
                        break;
                    case AggrFunc::Func::SUM:
                    case AggrFunc::Func::COUNT:
                        agggregates.push_back(Register::from_int(0));
                        break;
                    default:
                      __builtin_unreachable();
                    }
                }
            }
            size_t i = 0;
            while (i < aggr_funcs.size()) {
                if (aggr_funcs[i].func == AggrFunc::Func::MIN)
                    agggregates[i] = std::min(agggregates[i], *input_regs[aggr_funcs[i].attr_index]);
                else if (aggr_funcs[i].func ==  AggrFunc::Func::MAX)
                    agggregates[i] = std::max(agggregates[i], *input_regs[aggr_funcs[i].attr_index]);
                else if (aggr_funcs[i].func == AggrFunc::Func::SUM)
                    agggregates[i] = Register::from_int(agggregates[i].as_int() + input_regs[aggr_funcs[i].attr_index]->as_int());
                else if (aggr_funcs[i].func == AggrFunc::Func::COUNT)
                    agggregates[i] = Register::from_int(agggregates[i].as_int() + 1);
                i++;
            }

        }
        output_iterator = ht.begin();
        seen_input = true;
    }

    if (output_iterator == ht.end()) {
        return false;
    } else {
        size_t i = 0;
        while (i < group_by_attrs.size()) {
            output_regs[i] = output_iterator->first[i];
            i++;
        }
        i = 0;
        while (i < aggr_funcs.size()) {
            output_regs[group_by_attrs.size() + i] = output_iterator->second[i];
            i++;
        }
        return true;
    }
}

void HashAggregation::close() {
    input->close();
    ht.clear();
}

std::vector<Register*> HashAggregation::get_output() {
    std::vector<Register*> output;
    for (auto& reg : output_regs) {
        output.push_back(&reg);
    }
    return output;
}

Union::Union(Operator& input_left, Operator& input_right) : BinaryOperator(input_left, input_right) {}

Union::~Union() = default;

void Union::open() {
    input_left->open();
    input_right->open();
    input_regs_left = input_left->get_output();
    input_regs_right = input_right->get_output();
    output_regs.resize(input_regs_left.size());
}

bool Union::next() {
    if (!seen_input) {
        while (input_left->next()) {
            std::vector<Register> tuple;
            for (auto& reg : input_regs_left) {
                tuple.push_back(*reg);
            }
            if (ht.insert(tuple).second) {
                size_t i = 0;
                while (i < input_regs_left.size()) {
                    output_regs[i] = *input_regs_left[i];
                    i++;
                }
                return true;
            }
        }
        seen_input = true;
    }
    while (input_right->next()) {
        std::vector<Register> tuple;
        for (auto& reg : input_regs_right) {
            tuple.push_back(*reg);
        }
        if (ht.insert(tuple).second) {
            size_t i = 0;
            while (i < input_regs_right.size()) {
                output_regs[i] = *input_regs_right[i];
                i++;
            }
            return true;
        }
}
    return false;
}

std::vector<Register*> Union::get_output() {
  std::vector<Register*> output;
  for (auto& reg : output_regs) {
      output.push_back(&reg);
  }
  return output;
}

void Union::close() {
    input_left->close();
    input_right->close();
    ht.clear();
}

UnionAll::UnionAll(Operator& input_left, Operator& input_right) : BinaryOperator(input_left, input_right) {}

UnionAll::~UnionAll() = default;

void UnionAll::open() {
    input_left->open();
    input_right->open();
    input_regs_left = input_left->get_output();
    input_regs_right = input_right->get_output();
    output_regs.resize(input_regs_left.size());
}

bool UnionAll::next() {
    if (!seen_input) {
        if (input_left->next()) {
            size_t i = 0;
            while (i < output_regs.size()) {
                output_regs[i] = *input_regs_left[i];
                i++;
            }
            return true;
        }
        seen_input = true;
    }
    if (input_right->next()) {
        size_t i = 0; 
        while (i < output_regs.size()) {
            output_regs[i] = *input_regs_right[i];
            i++;
        }
        return true;
    } else {
        return false;
    }
}

std::vector<Register*> UnionAll::get_output() {
  std::vector<Register*> output;
  for (auto& reg : output_regs) {
      output.push_back(&reg);
  }
  return output;
}

void UnionAll::close() {
    input_left->close();
    input_right->close();
}

Intersect::Intersect(Operator& input_left, Operator& input_right) : BinaryOperator(input_left, input_right) {}

Intersect::~Intersect() = default;

void Intersect::open() {
    input_left->open();
    input_right->open();
    input_regs_left = input_left->get_output();
    input_regs_right = input_right->get_output();
    output_regs.resize(input_regs_left.size());
}

bool Intersect::next() {
    if (!seen_input) {
        while (input_left->next()) {
          std::vector<Register> tuple;
          for (auto& reg : input_regs_left) {
              tuple.push_back(*reg);
          }
          auto& counter = ht[tuple];
          counter = 1; 
        }
        seen_input = true;
    }
    while (input_right->next()) {
        std::vector<Register> tuple;
        for (auto& reg : input_regs_right)
            tuple.push_back(*reg);
        if (ht.find(tuple) != ht.end() && ht.find(tuple)->second > 0) {
            --ht.find(tuple)->second;
            size_t i = 0;
            while (i < output_regs.size()) {
                output_regs[i] = tuple[i];\
                i++;
            }
            return true;
      }
    }
    return false;
}

std::vector<Register*> Intersect::get_output() {
    std::vector<Register*> output;
    for (auto& reg : output_regs) {
        output.push_back(&reg);
    }
    return output;
}

void Intersect::close() {
    input_left->close();
    input_right->close();
    ht.clear();
}

IntersectAll::IntersectAll(Operator& input_left, Operator& input_right) : BinaryOperator(input_left, input_right) {}

IntersectAll::~IntersectAll() = default;

void IntersectAll::open() {
    input_left->open();
    input_right->open();
    input_regs_left = input_left->get_output();
    input_regs_right = input_right->get_output();
    output_regs.resize(input_regs_left.size());
}

bool IntersectAll::next() {
    if (!seen_input) {
        while (input_left->next()) {
          std::vector<Register> tuple;
          for (auto& reg : input_regs_left) {
              tuple.push_back(*reg);
          }
          auto& counter = ht[tuple];
          ++counter;
        }
        seen_input = true;
    }
    while (input_right->next()) {
      std::vector<Register> tuple;
      for (auto& reg : input_regs_right) {
          tuple.push_back(*reg);
      }
        if (ht.find(tuple) != ht.end() && ht.find(tuple)->second > 0) {
            --ht.find(tuple)->second;
            size_t i = 0;
            while (i < output_regs.size()) {
                output_regs[i] = tuple[i];
                i++;
            }
            return true;
        }
    }
    return false;
}

std::vector<Register*> IntersectAll::get_output() {
  std::vector<Register*> output;
  for (auto& reg : output_regs) {
      output.push_back(&reg);
  }
  return output;
}

void IntersectAll::close() {
    input_left->close();
    input_right->close();
    ht.clear();
}

Except::Except(Operator& input_left, Operator& input_right) : BinaryOperator(input_left, input_right) {}

Except::~Except() = default;

void Except::open() {
    input_left->open();
    input_right->open();
    input_regs_left = input_left->get_output();
    input_regs_right = input_right->get_output();
    output_regs.resize(input_regs_left.size());
}

bool Except::next() {
    if (!seen_input) {
        while (input_left->next()) {
            std::vector<Register> tuple;
            for (auto& reg : input_regs_left) {
                tuple.push_back(*reg);
            }
            auto& counter = ht[tuple];
            counter = 1;
        }
        while (input_right->next()) {
          std::vector<Register> tuple;
          for (auto& reg : input_regs_right) {
            tuple.push_back(*reg);
          }
          if (ht.find(tuple) != ht.end() && ht.find(tuple)->second > 0) {
            --ht.find(tuple)->second;
          }
        }
        current_output_it = ht.begin();
        seen_input = true;
    }

    while (current_output_it != ht.end()) {
        auto& counter = current_output_it->second;
        if (counter > 0) {
            --counter;
            for (size_t i = 0; i < output_regs.size(); ++i) {
                output_regs[i] = current_output_it->first[i];
            }
            return true;
        }
        ++current_output_it;
    }
    return false;
}

std::vector<Register*> Except::get_output() {
    std::vector<Register*> output;
    for (auto& reg : output_regs) {
        output.push_back(&reg);
    }
    return output;
}

void Except::close() {
    input_left->close();
    input_right->close();
    ht.clear();
}

ExceptAll::ExceptAll(Operator& input_left, Operator& input_right) : BinaryOperator(input_left, input_right) {}

ExceptAll::~ExceptAll() = default;

void ExceptAll::open() {
    input_left->open();
    input_right->open();
    input_regs_left = input_left->get_output();
    input_regs_right = input_right->get_output();
    output_regs.resize(input_regs_left.size());
}

bool ExceptAll::next() {
    if (!seen_input) {
        while (input_left->next()) {
          std::vector<Register> tuple;
          for (auto& reg : input_regs_left) {
              tuple.push_back(*reg);
          }
            auto& counter = ht[tuple];
            ++counter;
        }
        while (input_right->next()) {
          std::vector<Register> tuple;
          for (auto& reg : input_regs_right) {
              tuple.push_back(*reg);
          }
          if (ht.find(tuple) != ht.end() && ht.find(tuple)->second > 0) {
            --ht.find(tuple)->second;
          }
        }
        current_output_it = ht.begin();
        seen_input = true;
    }

    while (current_output_it != ht.end()) {
        auto& counter = current_output_it->second;
        if (counter > 0) {
            --counter;
            size_t i = 0; 
            while (i < output_regs.size()) {
                output_regs[i] = current_output_it->first[i];
                i++;
            }
            return true;
        }
        ++current_output_it;
    }
    return false;
}

std::vector<Register*> ExceptAll::get_output() {
    std::vector<Register*> output;
    for (auto& reg : output_regs) {
        output.push_back(&reg);
    }
    return output;
}

void ExceptAll::close() {
    input_left->close();
    input_right->close();
    ht.clear();
}


}  // namespace operators
}  // namespace buzzdb
