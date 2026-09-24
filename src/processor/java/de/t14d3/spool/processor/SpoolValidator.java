package de.t14d3.spool.processor;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import java.io.IOException;
import java.io.Writer;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Opt-in, validate-only annotation processor.
 * <p>
 * It fails {@code javac} when an entity mapping is invalid, using the same
 * rules as the runtime {@code EntityMetadata} validator, and emits a GraalVM
 * {@code reflect-config.json} for the entity classes so native images can
 * instantiate and populate them.
 * <p>
 * It never generates source, SQL or runtime behavior changes. The runtime
 * validator remains authoritative; the two are kept in sync by the rule list
 * and by compiling the test entity corpus through this processor.
 */
@SupportedAnnotationTypes({
        "de.t14d3.spool.annotations.Entity",
        "de.t14d3.spool.annotations.MappedSuperclass"
})
@SupportedSourceVersion(SourceVersion.RELEASE_21)
public class SpoolValidator extends AbstractProcessor {

    private final Set<String> reflectEntries = new LinkedHashSet<>();

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        Elements elements = processingEnv.getElementUtils();
        for (Element element : roundEnv.getElementsAnnotatedWith(
                elements.getTypeElement("de.t14d3.spool.annotations.Entity"))) {
            if (element instanceof TypeElement type) {
                validateEntity(type);
                reflectEntries.add(type.getQualifiedName().toString());
            }
        }
        for (Element element : roundEnv.getElementsAnnotatedWith(
                elements.getTypeElement("de.t14d3.spool.annotations.MappedSuperclass"))) {
            if (element instanceof TypeElement type) {
                reflectEntries.add(type.getQualifiedName().toString());
            }
        }
        if (roundEnv.processingOver()) {
            writeReflectConfig();
        }
        return false;
    }

    private void validateEntity(TypeElement entity) {
        Elements elements = processingEnv.getElementUtils();
        Types types = processingEnv.getTypeUtils();

        validateSuperclasses(entity);

        java.util.List<VariableElement> fields = allInstanceFields(entity);
        java.util.List<VariableElement> idFields = new java.util.ArrayList<>();
        int autoIncrementCount = 0;
        for (VariableElement field : fields) {
            if (annotation(field, "de.t14d3.spool.annotations.Id") != null) {
                idFields.add(field);
                if (booleanValue(annotation(field, "de.t14d3.spool.annotations.Id"), "autoIncrement", elements)) {
                    autoIncrementCount++;
                }
            }
        }
        if (idFields.isEmpty()) {
            error(entity, "Entity " + entity.getQualifiedName() + " must have a field annotated with @Id");
        }
        if (idFields.size() > 1 && autoIncrementCount > 0) {
            error(entity, "Entity " + entity.getQualifiedName()
                    + " has a composite id; generated (autoIncrement) ids are only supported for single-column ids");
        }

        for (VariableElement field : fields) {
            validateField(entity, field, types, elements);
        }
    }

    private void validateSuperclasses(TypeElement entity) {
        TypeElement current = superclassElement(entity);
        while (current != null && !current.getQualifiedName().contentEquals("java.lang.Object")) {
            if (annotation(current, "de.t14d3.spool.annotations.Entity") != null) {
                error(entity, "Entity " + entity.getQualifiedName() + " extends entity "
                        + current.getQualifiedName() + "; entity inheritance is not supported. "
                        + "Share fields through @MappedSuperclass instead.");
                return;
            }
            if (annotation(current, "de.t14d3.spool.annotations.MappedSuperclass") == null) {
                for (Element enclosed : current.getEnclosedElements()) {
                    if (enclosed.getKind() == ElementKind.FIELD
                            && !enclosed.getModifiers().contains(Modifier.STATIC)) {
                        error(entity, "Entity " + entity.getQualifiedName() + " extends "
                                + current.getQualifiedName() + ", which is neither @Entity nor "
                                + "@MappedSuperclass; its field '" + enclosed.getSimpleName()
                                + "' would be ignored. Annotate the superclass with @MappedSuperclass.");
                        return;
                    }
                }
            }
            current = superclassElement(current);
        }
    }

    private void validateField(TypeElement entity, VariableElement field, Types types, Elements elements) {
        AnnotationMirror oneToMany = annotation(field, "de.t14d3.spool.annotations.OneToMany");
        AnnotationMirror manyToMany = annotation(field, "de.t14d3.spool.annotations.ManyToMany");
        AnnotationMirror oneToOne = annotation(field, "de.t14d3.spool.annotations.OneToOne");
        AnnotationMirror orderBy = annotation(field, "de.t14d3.spool.annotations.OrderBy");

        if (oneToMany != null) {
            if (!isCollection(field, types)) {
                error(field, "Entity " + entity.getQualifiedName() + " @OneToMany field '"
                        + field.getSimpleName() + "' must be a Collection");
                return;
            }
            String mappedBy = stringValue(oneToMany, "mappedBy", elements);
            TypeElement target = targetType(oneToMany, "targetEntity");
            boolean joinColumn = annotation(field, "de.t14d3.spool.annotations.JoinColumn") != null;
            if (mappedBy == null || mappedBy.isBlank()) {
                if (!joinColumn) {
                    error(field, "Entity " + entity.getQualifiedName() + " @OneToMany field '"
                            + field.getSimpleName() + "' has a blank mappedBy and no @JoinColumn; "
                            + "unidirectional @OneToMany needs @JoinColumn(name=\"...\") on the target table");
                }
            } else {
                if (joinColumn) {
                    error(field, "Entity " + entity.getQualifiedName() + " @OneToMany field '"
                            + field.getSimpleName() + "' declares both mappedBy='" + mappedBy
                            + "' and @JoinColumn; @JoinColumn is only used for unidirectional @OneToMany");
                }
                if (target != null) {
                    VariableElement inverse = findField(target, mappedBy);
                    if (inverse == null) {
                        error(field, "Entity " + entity.getQualifiedName() + " @OneToMany field '"
                                + field.getSimpleName() + "' declares mappedBy='" + mappedBy + "' but "
                                + target.getQualifiedName() + " has no such field");
                    } else if (annotation(inverse, "de.t14d3.spool.annotations.ManyToOne") == null) {
                        error(field, "Entity " + entity.getQualifiedName() + " @OneToMany field '"
                                + field.getSimpleName() + "' declares mappedBy='" + mappedBy + "' but "
                                + target.getQualifiedName() + "." + mappedBy + " is not annotated with @ManyToOne");
                    }
                }
            }
            validateOrderBy(entity, field, orderBy, target, elements);
            return;
        }

        if (manyToMany != null) {
            if (!isCollection(field, types)) {
                error(field, "Entity " + entity.getQualifiedName() + " @ManyToMany field '"
                        + field.getSimpleName() + "' must be a Collection");
                return;
            }
            TypeElement target = collectionElementType(field, types);
            if (target == null) {
                error(field, "Entity " + entity.getQualifiedName() + " @ManyToMany field '"
                        + field.getSimpleName() + "' must declare its element type (Collection<T>)");
                return;
            }
            String mappedBy = stringValue(manyToMany, "mappedBy", elements);
            if (mappedBy != null && !mappedBy.isBlank()) {
                VariableElement inverse = findField(target, mappedBy);
                if (inverse == null) {
                    error(field, "Entity " + entity.getQualifiedName() + " @ManyToMany field '"
                            + field.getSimpleName() + "' declares mappedBy='" + mappedBy + "' but "
                            + target.getQualifiedName() + " has no such field");
                } else if (annotation(inverse, "de.t14d3.spool.annotations.ManyToMany") == null
                        || !isCollection(inverse, types)) {
                    error(field, "Entity " + entity.getQualifiedName() + " @ManyToMany field '"
                            + field.getSimpleName() + "' declares mappedBy='" + mappedBy + "' but "
                            + target.getQualifiedName() + "." + mappedBy + " is not a @ManyToMany collection");
                }
            }
            validateOrderBy(entity, field, orderBy, target, elements);
            return;
        }

        if (oneToOne != null) {
            String mappedBy = stringValue(oneToOne, "mappedBy", elements);
            if (mappedBy != null && !mappedBy.isBlank()) {
                TypeElement target = declaredTypeElement(field.asType());
                if (target != null) {
                    VariableElement owner = findField(target, mappedBy);
                    if (owner == null) {
                        error(field, "Entity " + entity.getQualifiedName() + " @OneToOne field '"
                                + field.getSimpleName() + "' declares mappedBy='" + mappedBy + "' but "
                                + target.getQualifiedName() + " has no such field");
                    } else {
                        AnnotationMirror ownerAnnotation =
                                annotation(owner, "de.t14d3.spool.annotations.OneToOne");
                        if (ownerAnnotation == null) {
                            error(field, "Entity " + entity.getQualifiedName() + " @OneToOne field '"
                                    + field.getSimpleName() + "' declares mappedBy='" + mappedBy + "' but "
                                    + target.getQualifiedName() + "." + mappedBy + " is not annotated with @OneToOne");
                        } else if (!stringValue(ownerAnnotation, "mappedBy", elements).isBlank()) {
                            error(field, "Entity " + entity.getQualifiedName() + " @OneToOne field '"
                                    + field.getSimpleName() + "' declares mappedBy='" + mappedBy + "' but "
                                    + target.getQualifiedName() + "." + mappedBy
                                    + " is itself an inverse side; mappedBy must point at the owning side");
                        }
                    }
                }
                if (booleanValue(oneToOne, "orphanRemoval", elements)) {
                    error(field, "Entity " + entity.getQualifiedName() + " @OneToOne field '"
                            + field.getSimpleName() + "' is the inverse side; orphanRemoval is only supported "
                            + "on the owning side");
                }
            }
            return;
        }

        if (orderBy != null) {
            error(field, "Entity " + entity.getQualifiedName() + " @OrderBy is only valid on "
                    + "@OneToMany/@ManyToMany fields, but '" + field.getSimpleName()
                    + "' is not a collection relationship");
        }
    }

    private void validateOrderBy(TypeElement entity, VariableElement field, AnnotationMirror orderBy,
                                 TypeElement target, Elements elements) {
        if (orderBy == null || target == null) {
            return;
        }
        String value = stringValue(orderBy, "value", elements);
        if (value == null || value.isBlank()) {
            return;
        }
        for (String segment : value.split(",")) {
            String[] tokens = segment.trim().split("\\s+");
            if (tokens.length == 0 || tokens.length > 2
                    || (tokens.length == 2
                    && !tokens[1].equalsIgnoreCase("ASC") && !tokens[1].equalsIgnoreCase("DESC"))) {
                error(field, "Entity " + entity.getQualifiedName() + " @OrderBy on field '"
                        + field.getSimpleName() + "' must be 'field [ASC|DESC][, ...]'");
                return;
            }
            VariableElement ordered = findField(target, tokens[0]);
            if (ordered == null) {
                error(field, "Entity " + entity.getQualifiedName() + " @OrderBy on field '"
                        + field.getSimpleName() + "' references unknown field "
                        + target.getQualifiedName() + "." + tokens[0]);
                return;
            }
            boolean columnMapped = annotation(ordered, "de.t14d3.spool.annotations.Column") != null
                    || annotation(ordered, "de.t14d3.spool.annotations.Id") != null
                    || annotation(ordered, "de.t14d3.spool.annotations.ManyToOne") != null
                    || annotation(ordered, "de.t14d3.spool.annotations.OneToOne") != null;
            if (!columnMapped) {
                error(field, "Entity " + entity.getQualifiedName() + " @OrderBy on field '"
                        + field.getSimpleName() + "' references " + target.getQualifiedName() + "."
                        + tokens[0] + ", which is not a mapped column");
                return;
            }
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static java.util.List<VariableElement> allInstanceFields(TypeElement type) {
        java.util.List<VariableElement> fields = new java.util.ArrayList<>();
        for (TypeElement current = type; current != null
                && !current.getQualifiedName().contentEquals("java.lang.Object");
             current = superclassElement(current)) {
            for (Element enclosed : current.getEnclosedElements()) {
                if (enclosed.getKind() == ElementKind.FIELD
                        && !enclosed.getModifiers().contains(Modifier.STATIC)) {
                    fields.add((VariableElement) enclosed);
                }
            }
        }
        return fields;
    }

    private static VariableElement findField(TypeElement type, String name) {
        for (TypeElement current = type; current != null
                && !current.getQualifiedName().contentEquals("java.lang.Object");
             current = superclassElement(current)) {
            for (Element enclosed : current.getEnclosedElements()) {
                if (enclosed.getKind() == ElementKind.FIELD
                        && enclosed.getSimpleName().contentEquals(name)) {
                    return (VariableElement) enclosed;
                }
            }
        }
        return null;
    }

    private static TypeElement superclassElement(TypeElement type) {
        TypeMirror superclass = type.getSuperclass();
        if (superclass instanceof DeclaredType declared
                && declared.asElement() instanceof TypeElement element) {
            return element;
        }
        return null;
    }

    private static TypeElement declaredTypeElement(TypeMirror type) {
        if (type instanceof DeclaredType declared
                && declared.asElement() instanceof TypeElement element) {
            return element;
        }
        return null;
    }

    private TypeElement collectionElementType(VariableElement field, Types types) {
        TypeMirror type = field.asType();
        if (type instanceof DeclaredType declared && !declared.getTypeArguments().isEmpty()) {
            TypeMirror argument = declared.getTypeArguments().get(0);
            TypeElement element = declaredTypeElement(argument);
            if (element != null) {
                return element;
            }
        }
        return null;
    }

    private boolean isCollection(VariableElement field, Types types) {
        TypeElement collection = processingEnv.getElementUtils().getTypeElement("java.util.Collection");
        if (collection == null) {
            return false;
        }
        return types.isAssignable(types.erasure(field.asType()), types.erasure(collection.asType()));
    }

    private static AnnotationMirror annotation(Element element, String annotationName) {
        for (AnnotationMirror mirror : element.getAnnotationMirrors()) {
            if (((TypeElement) mirror.getAnnotationType().asElement()).getQualifiedName()
                    .contentEquals(annotationName)) {
                return mirror;
            }
        }
        return null;
    }

    private String stringValue(AnnotationMirror mirror, String name, Elements elements) {
        if (mirror == null) {
            return "";
        }
        for (Map.Entry<? extends javax.lang.model.element.ExecutableElement,
                ? extends javax.lang.model.element.AnnotationValue> entry
                : elements.getElementValuesWithDefaults(mirror).entrySet()) {
            if (entry.getKey().getSimpleName().contentEquals(name)) {
                Object value = entry.getValue().getValue();
                return value == null ? "" : value.toString();
            }
        }
        return "";
    }

    private boolean booleanValue(AnnotationMirror mirror, String name, Elements elements) {
        if (mirror == null) {
            return false;
        }
        for (Map.Entry<? extends javax.lang.model.element.ExecutableElement,
                ? extends javax.lang.model.element.AnnotationValue> entry
                : elements.getElementValuesWithDefaults(mirror).entrySet()) {
            if (entry.getKey().getSimpleName().contentEquals(name)) {
                Object value = entry.getValue().getValue();
                return value instanceof Boolean bool && bool;
            }
        }
        return false;
    }

    private TypeElement targetType(AnnotationMirror mirror, String name) {
        if (mirror == null) {
            return null;
        }
        for (Map.Entry<? extends javax.lang.model.element.ExecutableElement,
                ? extends javax.lang.model.element.AnnotationValue> entry
                : processingEnv.getElementUtils().getElementValuesWithDefaults(mirror).entrySet()) {
            if (entry.getKey().getSimpleName().contentEquals(name)) {
                Object value = entry.getValue().getValue();
                if (value instanceof TypeMirror typeMirror) {
                    return declaredTypeElement(typeMirror);
                }
            }
        }
        return null;
    }

    private void error(Element element, String message) {
        if (element == null) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, message);
        } else {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.ERROR, message, element);
        }
    }

    private void writeReflectConfig() {
        if (reflectEntries.isEmpty()) {
            return;
        }
        StringBuilder json = new StringBuilder("[\n");
        int index = 0;
        for (String entry : reflectEntries) {
            if (index++ > 0) {
                json.append(",\n");
            }
            json.append("  {\n")
                    .append("    \"name\": \"").append(entry).append("\",\n")
                    .append("    \"allDeclaredConstructors\": true,\n")
                    .append("    \"allDeclaredFields\": true,\n")
                    .append("    \"allDeclaredMethods\": true\n")
                    .append("  }");
        }
        json.append("\n]\n");

        try {
            FileObject resource = processingEnv.getFiler().createResource(
                    StandardLocation.CLASS_OUTPUT, "",
                    "META-INF/native-image/de.t14d3/spool/reflect-config.json");
            try (Writer writer = resource.openWriter()) {
                writer.write(json.toString());
            }
        } catch (IOException e) {
            error(null, "Failed to write GraalVM reflect-config.json: " + e.getMessage());
        }
    }
}
